# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import time
from queue import Empty, Queue
from typing import Any

from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer
from recovery_libs.event_bus.event_bus_listener import EventBusListener

from watcher.lib.plugins.rabbitmq import EventBusPublisher
from watcher.lib.utils.constants import DEFAULT_QUEUE_TIMEOUT_SECS
from watcher.lib.utils.data import ActionCache, find_matching_regex_workflows, is_json
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import RABBITMQ_MESSAGES_EXCEPTIONS
from watcher.lib.utils.protobuf import deserialize_protobuf_logs
from watcher.lib.utils.workflow_parser import (
    parse_log_record_to_workflow_item,
    parse_otlp_json_to_workflow_items,
)
from watcher.models.audit import WatcherAnomalyAuditEvent
from watcher.models.config import WatcherConfig
from watcher.models.workflow import WorkflowItem
from watcher.workflow import RulesEngine


class WatcherService:
    """
    A service that processes events and executes workflows based on configured rules.

    The WatcherService is responsible for:
    1. Receiving and processing events from RabbitMQ
    2. Converting events into workflow items
    3. Matching workflow items against configured rules
    4. Executing matching workflows through the rules engine
    5. Running periodic maintenance tasks

    The service maintains a queue for event processing and uses an event loop
    for asynchronous operations. It supports both protobuf and JSON message formats.

    Attributes:
        config (WatcherConfig): Service configuration including rules and data sources
        engine (RulesEngine): Engine for executing workflow rules
        queue (Queue): Queue for receiving events from RabbitMQ
        publisher (EventBusPublisher): For publishing results back to RabbitMQ
        subscriber (EventBusListener): For subscribing to RabbitMQ events
        interrupted (bool): Flag indicating if service should stop
        loop (asyncio.AbstractEventLoop): Event loop for async operations
        last_maintenance_time (float): Timestamp of last maintenance run
    """

    def __init__(
        self,
        config: WatcherConfig,
        engine: RulesEngine,
        queue: Queue,
        publisher: EventBusPublisher,
        subscriber: EventBusListener,
    ):
        """
        Initialize the WatcherService.

        Args:
            config (WatcherConfig): Service configuration
            engine (RulesEngine): Rules engine for executing workflows
            queue (Queue): Queue for temporarily storing events
            publisher (EventBusPublisher): For publishing events
            subscriber (EventBusListener): For subscribing to events
        """
        self.config = config
        self.queue = queue
        self.engine = engine
        self.publisher = publisher
        self.subscriber = subscriber
        self.interrupted = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self.last_maintenance_time = 0

    @classmethod
    def make(
        cls,
        config: WatcherConfig,
        audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = NoOpAuditBuffer[WatcherAnomalyAuditEvent](),
    ) -> "WatcherService":
        """
        Create a new WatcherService instance with all required components.

        This factory method sets up:
        1. A queue for event processing
        2. A rules engine with KV store and action cache
        3. RabbitMQ subscriber for receiving events
        4. RabbitMQ publisher for sending results
        5. The main service instance

        Args:
            config (WatcherConfig): Service configuration

        Returns:
            WatcherService: A fully configured service instance
        """
        queue: Queue = Queue(maxsize=config.service.max_queue_size)
        engine_kvstore: dict[str, Any] = {}
        engine_action_cache: ActionCache = ActionCache(5 * 60, 60)  # 5 minutes TTL, 60 max size
        subscriber: EventBusListener = EventBusListener(
            config=config.datasources.rabbitmq.subscriber.to_event_bus_config(),  # type: ignore[union-attr]
            queue=queue,
            errors_counter=RABBITMQ_MESSAGES_EXCEPTIONS,
        )
        publisher: EventBusPublisher = EventBusPublisher(
            hostname=config.datasources.rabbitmq.publisher.host,  # type: ignore[union-attr]
            username=config.datasources.rabbitmq.publisher.username,  # type: ignore[union-attr]
            password=config.datasources.rabbitmq.publisher.password,  # type: ignore[union-attr]
            vhost=config.datasources.rabbitmq.publisher.vhost,  # type: ignore[union-attr]
            exchange=config.datasources.rabbitmq.publisher.exchange,  # type: ignore[union-attr]
            queues=config.datasources.rabbitmq.publisher.queues,  # type: ignore[union-attr]
            routing_keys=config.datasources.rabbitmq.publisher.routing_keys,  # type: ignore[union-attr]
        )
        engine: RulesEngine = RulesEngine(
            config=config,
            publisher=publisher,
            kvstore=engine_kvstore,
            action_cache=engine_action_cache,
            audit_event_buffer=audit_event_buffer,
        )
        return WatcherService(
            config=config,
            engine=engine,
            queue=queue,
            publisher=publisher,
            subscriber=subscriber,
        )

    async def process_workflow_item(self, item: WorkflowItem, workflows: list):
        """
        Process a single workflow item through matching workflows.

        This method executes the given workflow item through each matching workflow
        using the rules engine. It's called for each workflow item that matches
        the regex patterns defined in the configuration.

        Args:
            item (WorkflowItem): The workflow item to process
            workflows (list): List of matching workflows to execute
        """
        for wf in workflows:
            logger.info(f"processing workflow item: {wf} - {item}")
            wf_spec = self.config.find_workflow_by_name_type(name=wf["name"], type_=wf["type"])
            if wf_spec is None:
                logger.warning(f"workflow not found: {wf['name']} ({wf['type']}), skipping")
                continue
            await self.engine.execute_workflow(workflow=[wf_spec.to_json()], workflow_input=[item])

    async def run_maintenance_tasks(self):
        """
        Execute maintenance tasks for the service.

        This method runs periodic maintenance tasks through the rules engine.
        Maintenance tasks are executed at regular intervals defined in the
        service configuration (maintenance_tasks_interval_secs).

        Note: Publisher health checks are handled by RulesEngine.run_maintenance_tasks()
        which runs in both service and scheduler modes. This method only handles
        subscriber-specific maintenance for service mode.
        """
        try:
            # Check subscriber health (service mode only)
            await self.check_subscriber_health()

            # Run engine maintenance tasks (includes publisher health checks)
            self.engine.run_maintenance_tasks()
            logger.debug("maintenance tasks completed successfully")
        except Exception as e:
            logger.exception("error running maintenance tasks", extra={"error": str(e)})

    async def check_subscriber_health(self):
        """
        Check subscriber connection health (service mode only).

        The subscriber is only used in service mode, not scheduler mode.
        The EventBusListener handles its own reconnection, so we just log status.
        """
        try:
            subscriber_healthy = self.subscriber.is_healthy()

            if not subscriber_healthy:
                logger.warning("[Health Check] Subscriber connection is unhealthy")
                # The EventBusListener handles its own reconnection in the consume_messages loop
                # so we just log the issue here
            else:
                logger.debug("[Health Check] Subscriber connection is healthy")

        except Exception as e:
            logger.exception("[Health Check] Error checking subscriber health", extra={"error": str(e)})

    def setup_rabbitmq_connections(self, max_retries: int = 10, base_delay: int = 5) -> bool:
        """
        Set up RabbitMQ connections with retry logic.

        Args:
            max_retries (int): Maximum number of retry attempts
            base_delay (int): Base delay in seconds for exponential backoff

        Returns:
            bool: True if setup was successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"[RabbitMQ Setup] Attempt {attempt + 1}/{max_retries}")

                # Setup publisher connection
                logger.info("[RabbitMQ Setup] Setting up publisher connection...")
                self.publisher.connect()
                self.publisher.setup_channel()
                self.publisher.declare_exchange()
                self.publisher.setup_all_queues()
                self.publisher.configure_delivery_qos()

                # Setup subscriber connection
                logger.info("[RabbitMQ Setup] Setting up subscriber connection...")
                self.subscriber.start_consuming()

                logger.info("[RabbitMQ Setup] Successfully established RabbitMQ connections")
                return True

            except Exception as e:
                logger.exception(
                    f"[RabbitMQ Setup] Attempt {attempt + 1} failed", extra={"attempt": attempt + 1, "error": str(e)}
                )

                if attempt < max_retries - 1:
                    # Calculate exponential backoff delay
                    delay = base_delay * (2 ** (attempt + 1))
                    logger.info(f"[RabbitMQ Setup] Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(
                        f"[RabbitMQ Setup] All {max_retries} attempts failed. Unable to establish RabbitMQ connections."
                    )
                    return False

        return False

    def run(self):
        """
        Run the watcher service.

        This is the main service loop that:
        1. Sets up RabbitMQ connections and channels with retry logic
        2. Processes incoming messages from the queue
        3. Converts messages to workflow items
        4. Routes messages to workflows
        5. Runs maintenance tasks at configured intervals
        6. Handles service interruption and cleanup
        """
        # Setup RabbitMQ connections with retry logic
        if not self.setup_rabbitmq_connections():
            logger.error("Failed to establish RabbitMQ connections. Service will exit.")
            return

        # create event loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        while not self.interrupted:
            try:
                # Run maintenance tasks every maintenance_tasks_interval_secs seconds
                current_time = asyncio.get_event_loop().time()  # Returns time in seconds
                if current_time - self.last_maintenance_time >= self.config.service.maintenance_tasks_interval_secs:
                    self.loop.run_until_complete(self.run_maintenance_tasks())
                    self.last_maintenance_time = current_time

                # Process messages from the queue
                (routing_key, _payload_type, data) = self.queue.get(block=True, timeout=DEFAULT_QUEUE_TIMEOUT_SECS)
                logger.debug(f"Received {len(data)} bytes on {routing_key}")
                if self.config.datasources.rabbitmq.subscriber.is_payload_protobuf:
                    log_records = deserialize_protobuf_logs(data)
                    workflow_items = [parse_log_record_to_workflow_item(log_record=record) for record in log_records]
                else:
                    data = json.loads(data) if is_json(data) else data.decode()
                    workflow_items = parse_otlp_json_to_workflow_items(data)

                for item in workflow_items:
                    workflows_to_be_processed = find_matching_regex_workflows(
                        config=self.config, event=item.record.body or ""
                    )
                    self.loop.run_until_complete(
                        self.process_workflow_item(item=item, workflows=workflows_to_be_processed)
                    )

                self.queue.task_done()
            except KeyboardInterrupt:
                logger.info("received interrupt signal, stopping")
                self.interrupted = True
            except Empty:
                pass
            except Exception as e:
                logger.exception("error processing message", extra={"error": str(e)})
                self.queue.task_done()

        # cleanup event loop
        if self.loop:
            self.loop.close()

        # cleanup publisher connection
        if self.publisher.connection and self.publisher.connection.is_open:
            self.publisher.connection.close()

        # cleanup subscriber connection
        self.subscriber.stop_consuming()
