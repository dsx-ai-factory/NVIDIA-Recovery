# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import functools
import json
import threading
import time
from datetime import datetime, timedelta

import pika
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from pika.exceptions import ConnectionClosedByBroker
from prometheus_client import Counter, Histogram
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, ProducerEvent
from recovery_libs.event_bus import rabbit_eventing
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    JobAnomaly,
    SlurmJobExecutionStatus,
)
from recovery_libs.storage.dataclass import SlurmJobFinalStates
from recovery_libs.storage.node_events import ErrorAttributionType
from recovery_libs.tracing.recovery import recovery_context_from_headers, recovery_headers, recovery_scope
from recovery_libs.tracing.traces import (
    JobAnomalySpanName,
    extract_trace_context_from_rabbitmq_properties,
    populate_headers_with_span_context,
)
from recovery_libs.utils.retry import exponential_backoff_retry

from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor
from anomaly_detection_isolator.job_manager_client import JobManagerClient
from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger

EVENTBUS_ERROR_LABEL = "eventbus_error"
EVENTBUS_PUBLISH_TIME = Histogram(
    "adi_downstream_eventbus_publish_time", "Time taken (in seconds) to publish a message to event bus"
)
EVENTBUS_PUBLISH_ERRORS = Counter(
    "adi_downstream_eventbus_publish_errors", "Error publishing a message to event bus", [EVENTBUS_ERROR_LABEL]
)

SLOW_SIGNALS_MISSING_JOB_STATUS_COUNT = Counter(
    "adi_slow_signals_missing_job_status_count",
    "Number of jobs that are missing job status when slow signal message is received",
)
SLOW_SIGNALS_JOB_NOT_IN_FINAL_STATE_COUNT = Counter(
    "adi_slow_signals_job_not_in_final_state_count",
    "Number of jobs that are not in a final state when slow signal message is received",
)
FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS = Counter(
    "adi_fast_signals_anomaly_processor_errors",
    "Number of anomaly processor failures during fast signals processing",
    ["job_id", "job_attempt_index"],
)
SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS = Counter(
    "adi_slow_signals_anomaly_processor_errors",
    "Number of anomaly processor failures during slow signals processing",
    ["job_id", "job_attempt_index"],
)


tracer = trace.get_tracer(__name__)


class AnomalyDetectionIsolator:
    def __init__(
        self,
        settings: Settings,
        audit_event_buffer: AuditBuffer[ProducerEvent] = NoOpAuditBuffer[ProducerEvent](),
    ):
        self.settings = settings
        self.anomaly_processor = AnomalyProcessor(
            settings=self.settings,
            audit_event_buffer=audit_event_buffer,
        )
        self.job_manager_client = JobManagerClient(settings=self.settings)

    def _validate_job_status(self, job_status: SlurmJobExecutionStatus, delivery_tag: int) -> bool:
        """Validate that job_status contains all required fields for ADI processing.

        Args:
            job_status: The job status object from Job Manager
            delivery_tag: Worker thread identifier for logging

        Returns:
            bool: True if all required fields are present, False otherwise
        """
        required_fields = {
            "job_id": job_status.job_id,
            "job_attempt_index": job_status.job_attempt_index,
            "start_time": job_status.start_time,
            "allocated_node_list": job_status.allocated_node_list,
            "anomalies": job_status.anomalies,
        }

        # Check for missing or None required fields
        missing_fields = [field for field, value in required_fields.items() if value is None]

        if missing_fields:
            field_details = ", ".join([f"{field}={required_fields[field]}" for field in missing_fields])
            logger.error(
                f"[Worker][{delivery_tag}] Job status missing required fields: {missing_fields} ({field_details}) "
                f"for job_id={job_status.job_id}, job_attempt_index={job_status.job_attempt_index}"
            )
            return False

        # Check for empty allocated_node_list
        if not job_status.allocated_node_list:
            logger.error(
                f"[Worker][{delivery_tag}] Job status has empty allocated_node_list (allocated_node_list=[]) "
                f"for job_id={job_status.job_id}, job_attempt_index={job_status.job_attempt_index}"
            )
            return False

        # Check for empty anomalies list
        if not job_status.anomalies:
            logger.error(
                f"[Worker][{delivery_tag}] Job status has empty anomalies list (anomalies=[]) "
                f"for job_id={job_status.job_id}, job_attempt_index={job_status.job_attempt_index}"
            )
            return False

        # Warn about optional but commonly used fields for attribution
        optional_fields_for_attribution = {
            "cluster": job_status.cluster,
            "command": job_status.command,
            "job_name": job_status.job_name,
            "user": job_status.user,
            "node_count": job_status.node_count,
            "total_gpu_count": job_status.total_gpu_count,
        }

        missing_optional = [field for field, value in optional_fields_for_attribution.items() if value is None]
        if missing_optional:
            field_details = ", ".join([f"{field}=None" for field in missing_optional])
            logger.warning(
                f"[Worker][{delivery_tag}] Job status missing optional attribution fields: {missing_optional} ({field_details}) "
                f"for job_id={job_status.job_id}, job_attempt_index={job_status.job_attempt_index}. "
                "Attribution may be incomplete."
            )

        return True

    def _consume_event_bus_msg_attribution_request(self, channel, method, properties, body, args):
        """Callback called when eventbus message receieves a message.

        Args:
            channel (_type_): pika channel on which message is received.
            method (_type_): pika internal, contains delivery_tag.
            properties (_type_): pika internal, carries the message properties like headers.
            body (_type_): body of the message.
            args (_type_): arguments passed on the top via callback.
        """
        logger.info(f"[Service] Received an attribution request message from queue {body}")
        (connection, threads) = args
        t = threading.Thread(
            target=self._process_attribution_request_event,
            args=(connection, channel, method.delivery_tag, properties, body),
        )
        t.start()
        threads.append(t)

    def _consume_event_bus_msg_slow_signals(self, channel, method, properties, body, args):
        """Callback called when eventbus message receieves a message.

        Args:
            channel (_type_): pika channel on which message is received.
            method (_type_): pika internal, contains delivery_tag.
            properties (_type_): pika internal, not used here.
            body (_type_): body of the message.
            args (_type_): arguments passed on the top via callback.
        """
        logger.info(f"[Service] Received a slow signal message from queue {body}")
        (connection, threads) = args
        t = threading.Thread(
            target=self._process_slow_signals_event, args=(connection, channel, method.delivery_tag, body)
        )
        t.start()
        threads.append(t)

    @exponential_backoff_retry(
        max_retries=5, base_delay=2, exceptions=(ConnectionClosedByBroker, rabbit_eventing.EventBusError)
    )
    def _publish_result_message_to_event_bus(
        self, message: str, delivery_tag: int, output_queue: str | None = None
    ) -> None:
        """Publish attribution results to Job Manager or an explicit custom output queue.

        Job Manager owns the default result queue and binding. ADI declares only
        the result exchange, avoiding an unconsumed copy in the retired ARM queue.
        Explicit custom destinations retain their queue declaration and binding.
        Event bus connection errors are retried with exponential backoff.

        Args:
            message (str): actual event message to be published.
            delivery_tag (int): tag identifying the worker thread, 1 for first thread, 2 for second and so on.
            output_queue (str | None): optional output queue from the event message. If provided, this queue
                will be declared and bound using its name as the routing key.
        """
        start_time = time.time()
        channel = rabbit_eventing.connect(
            hostname=self.settings.rabbitmq_url,
            vhost=self.settings.rabbitmq_vhost,
            credentials=pika.PlainCredentials(
                self.settings.rabbitmq_user, self.settings.rabbitmq_password.get_secret_value()
            ),
        )

        exchange_name = self.settings.isolator_completed_exchange
        routing_key = output_queue or "anomaly.isolation.bad_nodes"

        rabbit_eventing.declare_exchange(channel=channel, exchange_name=exchange_name)
        if output_queue:
            rabbit_eventing.declare_queue(channel=channel, queue_name=output_queue)
            rabbit_eventing.bind_queue(
                channel=channel,
                queue_name=output_queue,
                exchange_name=exchange_name,
                routing_key=routing_key,
            )
            logger.info(
                f"[Worker][{delivery_tag}] Bound custom queue: {output_queue} to exchange: {exchange_name} "
                f"with routing key: {routing_key}"
            )

        trace_headers = populate_headers_with_span_context(span=trace.get_current_span())
        rabbit_eventing.publish_message(
            channel=channel,
            exchange_name=exchange_name,
            routing_key=routing_key,
            message_body=message,
            properties=pika.BasicProperties(headers=trace_headers),
        )
        end_time = time.time()

        EVENTBUS_PUBLISH_TIME.observe(float(end_time - start_time))

        logger.info(
            f"[Worker][{delivery_tag}] Published message {message} to {exchange_name} via routing key:  {routing_key}"
        )

    def _should_send_slow_signals(self, anomaly_type: str) -> bool:
        """
        Determine whether slow signals should be sent for the given anomaly type.

        Args:
            anomaly_type: The type of anomaly (e.g., 'STRAGGLER', 'CRASH', 'HANG')

        Returns:
            bool: True if slow signals should be sent, False otherwise
        """
        return bool(self.settings.slow_signals_enabled) and anomaly_type != EventType.STRAGGLER.value

    @exponential_backoff_retry(
        max_retries=5, base_delay=2, exceptions=(ConnectionClosedByBroker, rabbit_eventing.EventBusError)
    )
    def _publish_slow_signals_message_to_event_bus(self, message: str, delivery_tag: int, delay_in_ms: int) -> None:
        """Publishes the slow signal message to event bus. Retries with exponential backoff in case
        of event bus connection errors.
        This function is used to publish a message to the event bus that will be delayed by a certain amount of time.
        The message will be published to a temporary queue that will be deleted after the message is published.
        The message will be delayed by a certain amount of time, and then the message will be published to the slow_signals_exchange with the routing key of "job.updates.slow_signals".

        Args:
            message (str): The message to publish to the event bus.
            delivery_tag (int): The delivery tag of the message.
            delay_in_ms (int): The delay in milliseconds to publish the message to the slow_signals_exchange.

        Returns:
            None
        """
        start_time = time.time()
        channel = rabbit_eventing.connect(
            hostname=self.settings.rabbitmq_url,
            vhost=self.settings.rabbitmq_vhost,
            credentials=pika.PlainCredentials(
                self.settings.rabbitmq_user, self.settings.rabbitmq_password.get_secret_value()
            ),
        )

        logger.info(f"[Worker][{delivery_tag}] Declaring exchange: {self.settings.slow_signals_exchange}")

        rabbit_eventing.declare_exchange(channel=channel, exchange_name=self.settings.slow_signals_exchange)

        # name of the temporary queue for the slow signals message
        temp_id = f"delay_queue_{delivery_tag}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # temprary queue to delay the message
        # TODO: add this method to rabbit_eventing library
        channel.queue_declare(
            queue=temp_id,
            durable=False,
            auto_delete=True,
            arguments={
                # If the message delay is 10 mins, queue will be deleted in 100 mins
                "x-expires": delay_in_ms * 10,
                "x-message-ttl": delay_in_ms,
                "x-dead-letter-exchange": self.settings.slow_signals_exchange,
                "x-dead-letter-routing-key": "job.updates.slow_signals",
            },
        )

        logger.debug(
            f"[Worker][{delivery_tag}] Binding queue: {temp_id} to exchange: {self.settings.slow_signals_exchange} with routing key: {temp_id}"
        )
        # bind the temporary queue to the slow_signals_exchange with the routing key of the temporary queue
        rabbit_eventing.bind_queue(
            channel=channel,
            queue_name=temp_id,
            exchange_name=self.settings.slow_signals_exchange,
            routing_key=temp_id,
        )

        logger.info(f"[Worker][{delivery_tag}] Publishing message to temporary queue: {temp_id}")

        # publish the message to the temporary queue
        trace_headers = populate_headers_with_span_context(span=trace.get_current_span())
        rabbit_eventing.publish_message(
            channel=channel,
            exchange_name=self.settings.slow_signals_exchange,
            routing_key=temp_id,
            message_body=message,
            properties=pika.BasicProperties(headers=trace_headers),
        )

        channel.close()

        end_time = time.time()
        EVENTBUS_PUBLISH_TIME.observe(float(end_time - start_time))

    def _process_attribution_request_event(self, connection, channel, delivery_tag, properties, body):
        """
        Processes the event received by calling FACT and then publishing the attribution results.
        """

        if body is None:
            logger.error(f"[Worker][{delivery_tag}] Attribution request event received is None")
            return

        event_json = body.decode()
        event = json.loads(event_json)
        logger.info(f"[Worker][{delivery_tag}] Starting to work on task: {event}")

        if "job_id" not in event or "anomaly_time" not in event:
            logger.error(f"[Worker][{delivery_tag}] Event received didnt have job_id or anomaly_time {event}")
            return

        job_id = event["job_id"]
        job_attempt_index = event.get("job_attempt_index")
        anomaly_time = event["anomaly_time"]
        output_queue = event.get("output_queue")  # Extract output_queue from event if present

        # If the anomaly_time is not timezone aware, add the UTC timezone to it as FACT needs it to be timezone aware.
        datetime_anomaly_time = datetime.fromisoformat(anomaly_time)
        if datetime_anomaly_time.tzinfo is None:
            anomaly_time = datetime_anomaly_time.isoformat() + "Z"

        complete_replay_fields = {
            "allocated_nodes",
            "detection_time",
            "event_type",
            "start_time",
            "workflow_id",
        }
        if complete_replay_fields <= event.keys():
            # Complete replay events can bypass JobManager without synthesizing identity.
            job_status = SlurmJobExecutionStatus(
                job_id=job_id,
                job_attempt_index=job_attempt_index,
                workflow_id=event["workflow_id"],
                job_name="test_job",
                end_time=datetime.fromisoformat(anomaly_time),
                start_time=datetime.fromisoformat(event["start_time"]),
                command="srun --job-name=test_job --nodes=1 --ntasks-per-node=1 --cpus-per-task=1 --time=00:01:00 --partition=test_partition --account=test_account --wrap='sleep 10'",
                cluster="test_cluster",
                state="CANCELLED",
                user="adi_replay",
                allocated_node_list=event["allocated_nodes"],
                anomalies=[
                    JobAnomaly(
                        anomaly_type=event["event_type"], timestamp=datetime.fromisoformat(event["detection_time"])
                    )
                ],
            )
        else:
            # call Job Manager to fetch anomaly type, detection_time and allocated nodes
            job_status = self.job_manager_client.get_job_status(delivery_tag, job_id, job_attempt_index)
            if not job_status:
                logger.warning("Job status not found in Job Manager response, failing gracefully.")
                return

            # Validate job_status has required fields
            if not self._validate_job_status(job_status, delivery_tag):
                logger.warning(
                    f"[Worker][{delivery_tag}] Job status validation failed for job {job_id}. "
                    "Continuing with processing, but anomaly attribution may not work correctly."
                )

            # end_time is not always present for runai, falling back to anomaly_time (as done before)
            job_status.end_time = (
                job_status.end_time if job_status.end_time is not None else datetime.fromisoformat(anomaly_time)
            )

        if not job_status.anomalies:
            logger.error(f"[Worker][{delivery_tag}] No anomalies found in job status, skipping.")
            return

        job_anomalies = job_status.anomalies

        # wait for configured amount of time
        logger.info(f"[Worker][{delivery_tag}] Sleeping for {self.settings.epilogue_started_wait_seconds} seconds.")
        time.sleep(self.settings.epilogue_started_wait_seconds)

        processable_anomalies = job_anomalies

        num_processable_anomalies = len(processable_anomalies)

        trace_context = extract_trace_context_from_rabbitmq_properties(event_body=body, properties=properties)
        trace_context = recovery_context_from_headers(trace_context, properties.headers if properties else None)

        for idx, job_anomaly in enumerate(processable_anomalies):
            with (
                recovery_scope(trace_context),
                tracer.start_as_current_span(
                    name=f"job_anomaly.{JobAnomalySpanName.ATTRIBUTION}", kind=SpanKind.PRODUCER, context=trace_context
                ) as span,
            ):
                job_anomaly_type = job_anomaly.anomaly_type

                span.set_attribute("job.id", job_id)
                span.set_attribute("job.attempt.source", job_attempt_index)
                identity = recovery_headers(trace_context)
                if identity:
                    span.set_attribute("recovery.sequence.id", identity["recovery-sequence-id"])
                    span.set_attribute("recovery.operation", identity["recovery-operation"])
                span.set_attribute("job_id", job_id)
                span.set_attribute("job_attempt_index", job_attempt_index)
                span.set_attribute("job_anomaly.uuid", job_anomaly.uuid)
                span.set_attribute("job_anomaly.type", job_anomaly_type)
                span.set_attribute("job_anomaly.attribution_type", "fast_signals")

                logger.debug(f"[Worker][{delivery_tag}] Processing job anomaly: {job_anomaly}")
                anomaly_kwargs = {
                    "anomaly": job_anomaly,
                    "job_status": job_status,
                    "run_checkers": idx == (num_processable_anomalies - 1),
                }
                try:
                    result, attribution_id = self.anomaly_processor.process_anomaly_with_fast_signals(**anomaly_kwargs)
                except Exception as e:
                    span.record_exception(e)
                    FAST_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels(
                        job_id=job_id, job_attempt_index=str(job_attempt_index)
                    ).inc()
                    logger.exception(
                        f"[Worker][{delivery_tag}] Error in anomaly_processor.process_anomaly_with_fast_signals "
                        f"(job_id={job_id}, job_attempt_index={job_attempt_index}, anomaly_uuid={job_anomaly.uuid}, anomaly_type={job_anomaly_type})",
                        extra={
                            "job_id": job_id,
                            "job_attempt_index": job_attempt_index,
                            "anomaly_uuid": job_anomaly.uuid,
                            "error": str(e),
                        },
                    )
                    # Create empty result to continue with isolation_complete message
                    result = AnomalyAttributionResult(
                        job_id=job_id,
                        job_attempt_index=job_attempt_index,
                        anomaly_type=EventType[job_anomaly_type],
                        anomaly_uuid=job_anomaly.uuid,
                        attribution_type=ErrorAttributionType.FAST,
                        node_attributions={},
                        global_attributions={},
                    )
                    attribution_id = None

                if self._should_send_slow_signals(job_anomaly_type):
                    logger.info(f"[Worker][{delivery_tag}] Publishing slow signals message to event bus.")

                    self._publish_slow_signals_message_to_event_bus(
                        message=json.dumps(
                            {
                                "job_id": job_id,
                                "job_attempt_index": job_attempt_index,
                                "anomaly_type": job_anomaly_type,
                                "anomaly_uuid": job_anomaly.uuid,
                                "fact_attribution_id": attribution_id,
                            }
                        ),
                        delivery_tag=delivery_tag,
                        delay_in_ms=self.settings.slow_signals_delay_seconds * 1000,
                    )
                else:
                    if job_anomaly_type == EventType.STRAGGLER.value:
                        logger.info(
                            f"[Worker][{delivery_tag}] Skipping slow signals for STRAGGLER anomaly type. "
                            f"STRAGGLER anomalies are real-time performance issues that don't require post-mortem analysis."
                        )
                    else:
                        logger.info(
                            f"[Worker][{delivery_tag}] Using fast signals only. slow_signals_enabled is set to {self.settings.slow_signals_enabled} and job_status.state is {job_status.state}"
                        )

                if not result:
                    continue

                if idx != (num_processable_anomalies - 1) and not any(result.node_attributions.values()):
                    logger.error(f"[Worker][{delivery_tag}] No error attributions found in result, skipping.")
                    # skip publishing message unless its the last anomaly or has a bad node identified.
                    continue

                if not result.anomaly_type:
                    result.anomaly_type = EventType[job_anomaly_type]

                logger.info(f"[Worker][{delivery_tag}] Result of fast signals attribution: {result}")

                # publish message 'fast signals attribution complete'
                try:
                    self._publish_result_message_to_event_bus(result.to_json(), delivery_tag, output_queue)
                    span.set_status(StatusCode.OK)
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, "Failed to publish attribution result to event bus."))
                    EVENTBUS_PUBLISH_ERRORS.labels(**{EVENTBUS_ERROR_LABEL: type(e).__name__}).inc()
                    logger.exception(
                        f"[Worker][{delivery_tag}] Error publishing event to event_bus", extra={"error": str(e)}
                    )

        def ack_message(channel, delivery_tag):
            # TODO: add a manual ack to the consumed messages after changing consumption ack behavior.
            pass

        cb = functools.partial(ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def _process_slow_signals_event(self, connection, channel, delivery_tag, body):
        """
        Processes the event received by calling FACT and then publishing the attribution results.
        """

        if body is None:
            logger.error(f"[Worker][{delivery_tag}] Slow signals event received is None")
            return
        event_json = body.decode()
        event = json.loads(event_json)
        logger.info(f"[Worker][{delivery_tag}] Starting to work on slow signals task: {event}")

        if "job_id" not in event:
            logger.error(f"[Worker][{delivery_tag}] Event received didnt have job_id {event}")
            return

        if "fact_attribution_id" not in event:
            logger.error(f"[Worker][{delivery_tag}] Event received didnt have fact_attribution_id {event}")
            return

        job_id = event["job_id"]
        job_attempt_index = event.get("job_attempt_index", 0)
        fact_attribution_id = event.get("fact_attribution_id")
        anomaly_type = event.get("anomaly_type", None)
        anomaly_uuid = event.get("anomaly_uuid", None)
        job_status = self.job_manager_client.get_job_status(delivery_tag, job_id, job_attempt_index)
        next_attempt_job_status = self.job_manager_client.get_job_status(delivery_tag, job_id, job_attempt_index + 1)

        if not job_status:
            SLOW_SIGNALS_MISSING_JOB_STATUS_COUNT.inc()
            logger.warning(f"Job status for job {job_id} not found in Job Manager response, failing gracefully.")
            return

        if job_status.state not in SlurmJobFinalStates:
            SLOW_SIGNALS_JOB_NOT_IN_FINAL_STATE_COUNT.inc()
            logger.warning(f"Job {job_id} is not in a final state, skipping slow signals attribution.")
            return

        if not next_attempt_job_status:
            logger.warning(
                f"Could not find job status for next attempt ({job_attempt_index + 1}) of job {job_id} "
                f"in Job Manager response. Will use the current attempt's end time ({job_status.end_time}) "
                f"plus {self.settings.epilog_query_duration_minutes} minutes as the epilog end time."
            )
            epilog_end_time = job_status.end_time + timedelta(minutes=self.settings.epilog_query_duration_minutes)
        else:
            logger.info(
                f"Found job status for next attempt ({job_attempt_index + 1}) of job {job_id} "
                f"in Job Manager response, using next attempt start time ({next_attempt_job_status.start_time}) "
                f"as epilog end time."
            )
            epilog_end_time = next_attempt_job_status.start_time

        with tracer.start_as_current_span(
            name=f"job_anomaly.{JobAnomalySpanName.ATTRIBUTION}", kind=SpanKind.PRODUCER
        ) as span:
            try:
                span.set_attribute("job_id", job_id)
                span.set_attribute("job_attempt_index", job_attempt_index)
                span.set_attribute("job_anomaly.uuid", anomaly_uuid)
                span.set_attribute("job_anomaly.type", anomaly_type)
                span.set_attribute("job_anomaly.attribution_type", "slow_signals")

                result = self.anomaly_processor.process_anomaly_with_slow_signals(
                    job_status, fact_attribution_id, anomaly_type, anomaly_uuid, epilog_end_time
                )
            except Exception as e:
                SLOW_SIGNALS_ANOMALY_PROCESSOR_ERRORS.labels(
                    job_id=job_id, job_attempt_index=str(job_attempt_index)
                ).inc()
                logger.exception(
                    f"[Worker][{delivery_tag}] Error in anomaly_processor.process_anomaly_with_slow_signals "
                    f"(job_id={job_id}, job_attempt_index={job_attempt_index}, anomaly_uuid={anomaly_uuid}, anomaly_type={anomaly_type})",
                    extra={
                        "job_id": job_id,
                        "job_attempt_index": job_attempt_index,
                        "anomaly_uuid": anomaly_uuid,
                        "error": str(e),
                    },
                )
                # Create empty result to continue with isolation_complete message
                result = AnomalyAttributionResult(
                    job_id=job_id,
                    job_attempt_index=job_attempt_index,
                    anomaly_type=EventType[anomaly_type] if anomaly_type else EventType.UNKNOWN,
                    anomaly_uuid=anomaly_uuid,
                    attribution_type=ErrorAttributionType.SLOW,
                    node_attributions={},
                    global_attributions={},
                )
                span.record_exception(e)

            if not result:
                return

            logger.info(f"[Worker][{delivery_tag}] Result of slow signals attribution: {result}")

            # publish message 'slow signals attribution complete'
            try:
                self._publish_result_message_to_event_bus(result.to_json(), delivery_tag)
                span.set_status(StatusCode.OK)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "Failed to publish attribution result to event bus."))
                EVENTBUS_PUBLISH_ERRORS.labels(**{EVENTBUS_ERROR_LABEL: type(e).__name__}).inc()
                logger.exception(
                    f"[Worker][{delivery_tag}] Error publishing event to event_bus", extra={"error": str(e)}
                )

        def ack_message(channel, delivery_tag):
            # TODO: add a manual ack to the consumed messages after changing consumption ack behavior.
            pass

        cb = functools.partial(ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    @exponential_backoff_retry(
        max_retries=5, base_delay=2, exceptions=(ConnectionClosedByBroker, rabbit_eventing.EventBusError)
    )
    def setup_event_bus_and_start_consuming(self) -> None:
        """
        Sets up event bus and start consuming messages i.e. call _consume_event_bus_msg method
        on the message recieved. Retries with exponential backoff in case of event bus errors.
        """
        connection = rabbit_eventing.create_connection(
            self.settings.rabbitmq_url,
            vhost=self.settings.rabbitmq_vhost,
            credentials=pika.PlainCredentials(
                self.settings.rabbitmq_user, self.settings.rabbitmq_password.get_secret_value()
            ),
        )

        channel = connection.channel()

        rabbit_eventing.declare_exchange(channel, self.settings.attribution_request_exchange)
        rabbit_eventing.declare_exchange(channel, self.settings.slow_signals_exchange)
        rabbit_eventing.declare_queue(channel, self.settings.attribution_request_queue)
        rabbit_eventing.declare_queue(channel, self.settings.slow_signals_queue)

        rabbit_eventing.bind_queue(
            channel,
            queue_name=self.settings.attribution_request_queue,
            exchange_name=self.settings.attribution_request_exchange,
            routing_key="job.updates.attribution_request",
        )

        rabbit_eventing.bind_queue(
            channel,
            queue_name=self.settings.slow_signals_queue,
            exchange_name=self.settings.slow_signals_exchange,
            routing_key="job.updates.slow_signals",
        )

        logger.info(
            f"[Service] Starting to listen to queue: messages in {self.settings.attribution_request_queue} on {self.settings.attribution_request_exchange} and {self.settings.slow_signals_queue} on {self.settings.slow_signals_exchange}"
        )

        threads: list[threading.Thread] = []
        attribution_request_callback = functools.partial(
            self._consume_event_bus_msg_attribution_request, args=(connection, threads)
        )
        slow_signals_callback = functools.partial(self._consume_event_bus_msg_slow_signals, args=(connection, threads))

        rabbit_eventing.consume_multiple_messages(
            channel=channel,
            queue_callbacks={
                self.settings.attribution_request_queue: attribution_request_callback,
                self.settings.slow_signals_queue: slow_signals_callback,
            },
        )

        for thread in threads:
            thread.join()

        rabbit_eventing.close_consumer(channel=channel)
