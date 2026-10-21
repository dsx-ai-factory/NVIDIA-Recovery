# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import os
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime
from typing import Any

import uvicorn
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, create_audit_buffer
from recovery_libs.tracing.traces import set_up_trace_provider

from watcher.config import RabbitMQConnectionSettings
from watcher.lib.plugins.log_query import LogQueryBatcher, LogQueryPoller, create_log_query_client
from watcher.lib.plugins.rabbitmq import EventBusPublisher
from watcher.lib.utils.config import load_watcher_config
from watcher.lib.utils.constants import APP_INFO_ENV_VARS
from watcher.lib.utils.data import ActionCache
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import WORKFLOW_RUN, emit_app_info_metrics
from watcher.models.audit import WatcherAnomalyAuditEvent
from watcher.models.config import (
    LogQueryPolicyConfig,
    RabbitMQPublisherConfig,
    WatcherConfig,
    WatcherEventSource,
)
from watcher.service import WatcherService
from watcher.workflow import RulesEngine

# setup fastapi
app = FastAPI(
    title="Watcher Service",
    summary="Service to detect anomaly events in observability and forward it to Eventbus.",
    root_path="",
)

# expose metrics endpoint
Instrumentator().instrument(app).expose(app)

# setup environment and shared client resources
typed_config: WatcherConfig = load_watcher_config(os.environ["CONFIG_PATH"], os.environ.get("RULES_PATH"))


def _get_log_query_policy(config: WatcherConfig) -> LogQueryPolicyConfig:
    """Return common polling policy, synthesizing defaults for new-only config."""

    return config.datasources.selected_log_query_policy or LogQueryPolicyConfig()


log_query_policy = _get_log_query_policy(typed_config)
rabbitmq_publisher_config = RabbitMQConnectionSettings(
    host=typed_config.datasources.rabbitmq.publisher.host,  # type: ignore[union-attr]
    username=typed_config.datasources.rabbitmq.publisher.username,  # type: ignore[union-attr]
    password=typed_config.datasources.rabbitmq.publisher.password,  # type: ignore[union-attr]
    vhost=typed_config.datasources.rabbitmq.publisher.vhost,  # type: ignore[union-attr]
)
service: WatcherService | None = None

# kvstores
log_query_state_store: dict[str, Any] = {}
rules_engine_kvstore: dict[str, Any] = {}
rules_engine_action_cache: ActionCache = ActionCache(timeout=typed_config.service.notification_dedup_ttl_secs)

# Global poller instance
log_query_poller: LogQueryPoller | None = None
_log_query_poller_loop: asyncio.AbstractEventLoop | None = None
_log_query_poller_stop_future: Future[Any] | None = None
_log_query_poller_state_lock = threading.Lock()
_log_query_poller_ready = threading.Event()


def _prepare_log_query_poller_runtime() -> None:
    """Reset loop-bound poller state before starting its worker thread."""

    global log_query_poller
    global _log_query_poller_loop
    global _log_query_poller_stop_future

    with _log_query_poller_state_lock:
        log_query_poller = None
        _log_query_poller_loop = None
        _log_query_poller_stop_future = None
        _log_query_poller_ready.clear()


def _publish_log_query_poller_runtime(loop: asyncio.AbstractEventLoop, poller: LogQueryPoller) -> None:
    """Publish loop-bound state for thread-safe shutdown."""

    global log_query_poller
    global _log_query_poller_loop

    with _log_query_poller_state_lock:
        log_query_poller = poller
        _log_query_poller_loop = loop
        _log_query_poller_ready.set()


def _mark_log_query_poller_exited(loop: asyncio.AbstractEventLoop | None) -> None:
    """Clear loop-bound state and unblock any concurrent shutdown waiter."""

    global log_query_poller
    global _log_query_poller_loop

    with _log_query_poller_state_lock:
        if loop is None or _log_query_poller_loop is loop:
            log_query_poller = None
            _log_query_poller_loop = None
        _log_query_poller_ready.set()


async def _stop_log_query_poller_on_owner(poller: LogQueryPoller) -> None:
    """Stop a poller and retry owned-backend cleanup if stopping fails."""

    try:
        await poller.stop()
    except BaseException:
        try:
            await poller.log_query_batcher.close()
        except Exception as error:
            logger.exception(
                "[LogQueryPoller] Backend cleanup after stop failure also failed", extra={"error": str(error)}
            )
        raise


def _shutdown_log_query_poller(thread: threading.Thread, timeout_seconds: float = 30.0) -> None:
    """Stop the poller on its owning loop, stop that loop, and join its thread."""

    global _log_query_poller_stop_future

    if not _log_query_poller_ready.wait(timeout=timeout_seconds):
        logger.warning("[LogQueryPoller] Timed out waiting for poller startup during shutdown")

    with _log_query_poller_state_lock:
        poller = log_query_poller
        loop = _log_query_poller_loop
        stop_future = _log_query_poller_stop_future
        if stop_future is None and poller is not None and loop is not None and loop.is_running():
            stop_future = asyncio.run_coroutine_threadsafe(_stop_log_query_poller_on_owner(poller), loop)
            _log_query_poller_stop_future = stop_future

            def stop_loop_when_cleanup_finishes(_: Future[Any]) -> None:
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    # The worker may already have closed its loop after an
                    # independent failure. In that case there is nothing left
                    # for the shutdown thread to stop.
                    pass

            stop_future.add_done_callback(stop_loop_when_cleanup_finishes)

    if stop_future is not None:
        try:
            stop_future.result(timeout=timeout_seconds)
        except FutureTimeoutError:
            logger.error("[LogQueryPoller] Timed out waiting for poller shutdown; cleanup remains scheduled")
        except Exception as error:
            logger.exception("[LogQueryPoller] Poller shutdown failed", extra={"error": str(error)})

    thread.join(timeout=timeout_seconds)
    if thread.is_alive():
        logger.error("[LogQueryPoller] Poller thread did not exit before the shutdown deadline")


def _close_eventbus_publisher(publisher: EventBusPublisher) -> None:
    """Close the publisher connection owned by the poller thread."""

    connection = publisher.connection
    if connection is not None and not connection.is_closed:
        connection.close()


@app.get("/healthz")
async def healthcheck():
    return {"status": "OK"}


def _create_rabbitmq_publisher(
    publisher_config: RabbitMQPublisherConfig,
    connection_settings: RabbitMQConnectionSettings,
    max_retries: int = 10,
    base_delay: int = 5,
) -> EventBusPublisher:
    """
    Create and return a configured RabbitMQ publisher with retry logic.

    Args:
        publisher_config: Typed RabbitMQ publisher configuration.
        connection_settings: RabbitMQ connection settings (with env-var overrides).
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for exponential backoff.

    Returns:
        EventBusPublisher: RabbitMQ publisher.

    Raises:
        Exception: If all retry attempts fail.
    """
    eventbus_publisher = EventBusPublisher(
        hostname=connection_settings.host,
        username=connection_settings.username,
        password=connection_settings.password,
        vhost=connection_settings.vhost,
        exchange=publisher_config.exchange,
        queues=publisher_config.queues,
        routing_keys=publisher_config.routing_keys,
    )

    # Retry connection setup with exponential backoff
    for attempt in range(max_retries):
        try:
            logger.info(f"[RabbitMQ Publisher Setup] Attempt {attempt + 1}/{max_retries}")

            eventbus_publisher.connect()
            eventbus_publisher.setup_channel()
            eventbus_publisher.declare_exchange()
            eventbus_publisher.setup_all_queues()
            eventbus_publisher.configure_delivery_qos()

            logger.info("[RabbitMQ Publisher Setup] Successfully established RabbitMQ publisher connection")
            return eventbus_publisher

        except Exception as e:
            logger.exception(
                f"[RabbitMQ Publisher Setup] Attempt {attempt + 1} failed",
                extra={"attempt": attempt + 1, "error": str(e)},
            )

            if attempt < max_retries - 1:
                # Calculate exponential backoff delay
                delay = base_delay * (2 ** (attempt + 1))
                logger.info(f"[RabbitMQ Publisher Setup] Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(
                    f"[RabbitMQ Publisher Setup] All {max_retries} attempts failed. Unable to establish RabbitMQ publisher connection."
                )
                raise RuntimeError(
                    f"Failed to establish RabbitMQ publisher connection after {max_retries} attempts: {e}"
                )

    # This should never be reached, but added for completeness
    raise RuntimeError("Unexpected error in RabbitMQ publisher setup")


async def run_workflow(
    watcher_config: WatcherConfig,
    log_query_client: LogQueryBatcher,
    eventbus_publisher: EventBusPublisher,
    rules_engine_kvstore: dict,
    rules_engine_action_cache: ActionCache,
    audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = NoOpAuditBuffer[WatcherAnomalyAuditEvent](),
) -> None:
    WORKFLOW_RUN.inc()
    engine = RulesEngine(
        config=watcher_config,
        log_query_batcher=log_query_client,
        publisher=eventbus_publisher,
        kvstore=rules_engine_kvstore,
        action_cache=rules_engine_action_cache,
        audit_event_buffer=audit_event_buffer,
    )
    workflow_steps = [step.to_json() for step in watcher_config.rules[0].workflow]
    await engine.execute_workflow(workflow_steps)


async def poller_workflow_callback(
    config: dict[str, Any],
    start_time: datetime,
    end_time: datetime,
    log_query_client: LogQueryBatcher,
    eventbus_publisher: EventBusPublisher,
    rules_engine_kvstore: dict,
    rules_engine_action_cache: ActionCache,
    watcher_config: WatcherConfig,
    audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = NoOpAuditBuffer[WatcherAnomalyAuditEvent](),
) -> None:
    """
    Callback for LogQueryPoller to execute workflow with time-bounded queries.

    Args:
        config: Query configuration dict from workflow step.
        start_time: Start time for this chunk.
        end_time: End time for this chunk.
        log_query_client: Selected log-query backend batcher.
        eventbus_publisher: RabbitMQ publisher.
        rules_engine_kvstore: KV store for rules engine.
        rules_engine_action_cache: Action cache for deduplication.
        watcher_config: Typed watcher configuration.
    """
    WORKFLOW_RUN.inc()
    engine = RulesEngine(
        config=watcher_config,
        log_query_batcher=log_query_client,
        publisher=eventbus_publisher,
        kvstore=rules_engine_kvstore,
        action_cache=rules_engine_action_cache,
        audit_event_buffer=audit_event_buffer,
    )

    # Pass the chunk bounds down
    await engine.execute_workflow([config], start=start_time, end=end_time)


def start_log_query_poller(audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = NoOpAuditBuffer()) -> None:
    """
    Start the LogQueryPoller in a dedicated asyncio event loop.
    """
    global log_query_poller

    loop: asyncio.AbstractEventLoop | None = None
    poller_started = False
    try:
        log_query_client = create_log_query_client(
            state_store=log_query_state_store,
            log_query_policy=_get_log_query_policy(typed_config),
            log_query_config=typed_config.datasources.log_query,
        )
    except Exception as error:
        logger.exception("[LogQueryPoller] Failed to initialize log-query client", extra={"error": str(error)})
        _mark_log_query_poller_exited(loop)
        return

    try:
        # Setup RabbitMQ publisher with error handling. The log-query client is
        # already initialized, so every failure below must release it.
        eventbus_publisher = _create_rabbitmq_publisher(
            publisher_config=typed_config.datasources.rabbitmq.publisher,  # type: ignore[union-attr]
            connection_settings=rabbitmq_publisher_config,
        )
    except Exception as error:
        logger.exception(
            "[LogQueryPoller] Failed to establish RabbitMQ publisher connection", extra={"error": str(error)}
        )
        logger.error("[LogQueryPoller] Cannot continue without RabbitMQ publisher. Poller thread will exit.")
        try:
            asyncio.run(log_query_client.close())
        finally:
            _mark_log_query_poller_exited(loop)
        return

    try:
        # Create event loop for the poller
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Extract workflow configs as dicts for LogQueryPoller
        workflow_configs = [step.to_json() for step in typed_config.rules[0].workflow]

        # Create callback with bound parameters
        async def workflow_callback_bound(config: dict[str, Any], start_time: datetime, end_time: datetime) -> None:
            await poller_workflow_callback(
                config=config,
                start_time=start_time,
                end_time=end_time,
                log_query_client=log_query_client,
                eventbus_publisher=eventbus_publisher,
                rules_engine_kvstore=rules_engine_kvstore,
                rules_engine_action_cache=rules_engine_action_cache,
                watcher_config=typed_config,
                audit_event_buffer=audit_event_buffer,
            )

        # Create and start the poller
        log_query_poller = LogQueryPoller(
            log_query_batcher=log_query_client,
            workflow_callback=workflow_callback_bound,
            query_configs=workflow_configs,
            poll_interval_secs=15,
        )

        logger.info("[LogQueryPoller] Starting log-query poller...")
        loop.run_until_complete(log_query_poller.start())
        poller_started = True
        # Publish readiness from inside run_forever so shutdown cannot observe
        # a loop that exists but is not yet accepting thread-safe work.
        loop.call_soon(_publish_log_query_poller_runtime, loop, log_query_poller)
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("[LogQueryPoller] Received keyboard interrupt, stopping poller...")
    finally:
        try:
            if loop is None:
                asyncio.run(log_query_client.close())
            elif not loop.is_closed():
                if log_query_poller is not None:
                    try:
                        loop.run_until_complete(_stop_log_query_poller_on_owner(log_query_poller))
                    finally:
                        if not poller_started:
                            loop.run_until_complete(log_query_client.close())
                else:
                    loop.run_until_complete(log_query_client.close())
        finally:
            try:
                _close_eventbus_publisher(eventbus_publisher)
            finally:
                if loop is not None and not loop.is_closed():
                    loop.close()
                _mark_log_query_poller_exited(loop)


def _start_http_server() -> None:
    logger.info("Starting http server...")
    uvicorn.run(
        app,
        host=typed_config.service.host,
        port=typed_config.service.port,
        log_level=typed_config.service.http_log_level.lower(),
    )


def main() -> None:
    set_up_trace_provider(service_name="watcher")

    audit_buffer: AuditBuffer[WatcherAnomalyAuditEvent] = create_audit_buffer(
        typed_config.service.audit_trail,
        logger=logger.getChild("audit"),
    )
    try:
        audit_buffer.start()

        emit_app_info_metrics(environment_variables=APP_INFO_ENV_VARS)
        http_thread = threading.Thread(target=_start_http_server, daemon=True)
        http_thread.start()

        if typed_config.event_source == WatcherEventSource.RABBITMQ:
            global service
            service = WatcherService.make(
                typed_config,
                audit_event_buffer=audit_buffer,
            )
            service.run()
        elif typed_config.event_source.uses_log_query:
            # Start the dedicated log-query poller
            logger.info("[main] Starting log-query poller mode...")
            poller_thread = threading.Thread(
                target=start_log_query_poller,
                args=(audit_buffer,),
                daemon=True,
            )
            _prepare_log_query_poller_runtime()
            poller_thread.start()

            try:
                while poller_thread.is_alive():
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
            finally:
                _shutdown_log_query_poller(poller_thread)
                logger.info("Watcher Service finished, exiting")
        else:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                logger.info("Watcher Service finished, exiting")
    finally:
        audit_buffer.stop()


if __name__ == "__main__":
    main()
