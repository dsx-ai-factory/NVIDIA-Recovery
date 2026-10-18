# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import textwrap
from collections.abc import Callable
from typing import Annotated, Any, ClassVar, Literal, TypeVar

import chevron_blue
import pydantic

from notifications.models.base import BaseModel
from notifications.models.notification_types import NotificationType

_NotificationTemplateType = TypeVar("_NotificationTemplateType", bound="NotificationTemplate")


class NotificationTemplate(BaseModel):
    """The base class for notification templates.

    Each subclass represents a template and the expected format of the input data.

    Override TEMPLATE and TITLE_TEMPLATE to set the template. Add pydantic fields to specify the input data.

    The templates are in mustache format.
    """

    # Prevent templates from getting split into # *-Input and *-Output schemas.
    model_config = pydantic.ConfigDict(json_schema_mode_override="serialization")

    template_id: str = ""

    DEFAULT_TYPE: ClassVar[NotificationType] = NotificationType.info

    # Override this to set the template for the notification's title.
    TITLE_TEMPLATE: ClassVar[str] = ""

    # Override this to set the template for the notification's message.
    TEMPLATE: ClassVar[str] = ""

    # Override this to add mustache lambdas to the the render context.
    LAMBDAS: ClassVar[dict[str, Callable[..., Any]]] = {}

    # Do not override this. It's used internally to keep track of partials registered with `register_partial`.
    _PARTIALS: ClassVar[dict[str, type[NotificationTemplate]]] = {}

    def _get_data(self, override_partials):
        # Join the model data with the lambdas.
        data = {
            **self.model_dump(),
            **type(self).LAMBDAS,
        }
        if override_partials:
            for partial_cls in override_partials.values():
                data.update(partial_cls.LAMBDAS)
        return data

    def render_message(self, *, override_partials=None, template_transform=None) -> str:
        """Render the message.

        Args:
            override_partials: A dict of partial overrides.

        Returns:
            The rendered string message.
        """
        partials = type(self)._PARTIALS
        if override_partials is not None:
            partials = {
                **partials,
                **override_partials,
            }
        template = type(self).TEMPLATE
        if template_transform:
            template = template_transform(template)
        return chevron_blue.render(
            template=template,
            data=self._get_data(override_partials),
            partials_dict={key: value.TEMPLATE for key, value in partials.items()},
            warn=True,
        )

    def render_title(self, *, override_partials=None) -> str:
        """Render the title.

        Args:
            override_partials: A dict of partial overrides.

        Returns:
            The rendered string message.
        """
        partials = type(self)._PARTIALS
        if override_partials is not None:
            partials = {
                **partials,
                **override_partials,
            }
        return chevron_blue.render(
            template=type(self).TITLE_TEMPLATE,
            data=self._get_data(override_partials),
            partials_dict={key: value.TEMPLATE for key, value in partials.items()},
            warn=True,
        )

    @classmethod
    def register_partial(cls, partial_cls: type[_NotificationTemplateType]) -> type[_NotificationTemplateType]:
        """Mark a `NotificationTemplate` class as a partial.

        The registered partials will automatically be available in all NotificationTemplate templates as their class
        name.

        When rendering, you can override specific partials using the `override_partials` keyword argument.

        Args:
            partial_cls: The decorated class.

        Returns:
            The original class with no modifications.
        """
        class_name = partial_cls.__name__
        cls._PARTIALS[class_name] = partial_cls
        return partial_cls


@NotificationTemplate.register_partial
class JobRepr(NotificationTemplate):
    """A representation of a job, including id and attempt index.

    Examples:
        >>> JobRepr(job_id="456", job_attempt_index=7).render_message()
        '456(7)'
    """

    template_id: Literal["/partials/JobRepr"] = "/partials/JobRepr"

    TEMPLATE = "{{job_id}}({{job_attempt_index}})"

    job_id: str
    job_attempt_index: int


@NotificationTemplate.register_partial
class JobName(NotificationTemplate):
    """A representation of the job name.

    The default template just shows the job name, but it can be overridden to show more interesting representations.
    """

    template_id: Literal["/partials/JobName"] = "/partials/JobName"
    TEMPLATE = "{{job_name}}"

    job_name: str


@NotificationTemplate.register_partial
class User(NotificationTemplate):
    """A representation of a user.

    The default template just shows the user, but it can be overridden to show more interesting representations.
    """

    template_id: Literal["/partials/User"] = "/partials/User"
    TEMPLATE = "{{user}}"

    user: str


@NotificationTemplate.register_partial
class JobExecutionError(NotificationTemplate):
    """
    A representation of a job execution error.
    """

    template_id: Literal["/partials/JobExecutionError"] = "/partials/JobExecutionError"
    TEMPLATE = textwrap.dedent(
        """\
        Error message:{{#error_message}} {{error_message}}{{/error_message}}
        Error signature: {{error_signature}}"""
    )

    error_message: str | None = None
    error_signature: str | None = None


@NotificationTemplate.register_partial
class NodeEvent(NotificationTemplate):
    """
    A representation of a node attribution event.
    """

    template_id: Literal["/partials/NodeEvent"] = "/partials/NodeEvent"
    TEMPLATE = textwrap.dedent(
        """\
        {{node}}:
          exclusion type '{{exclusion_type}}'
          error attribution '{{error_attribution}}'
          error attribution sources: {{error_attribution_sources}}
        """
    )

    node: str
    exclusion_type: str
    error_attribution: str
    error_attribution_sources: list[str]


@NotificationTemplate.register_partial
class GlobalEvent(NotificationTemplate):
    """
    A representation of a global attribution event (job-level, not tied to specific nodes).
    """

    template_id: Literal["/partials/GlobalEvent"] = "/partials/GlobalEvent"
    TEMPLATE = textwrap.dedent(
        """\
        error attribution '{{error_attribution}}'
        error attribution sources: {{error_attribution_sources}}
        """
    )

    error_attribution: str
    error_attribution_sources: list[str]


@NotificationTemplate.register_partial
class ErrorAttribution(NotificationTemplate):
    """
    A representation of an error attribution that's made of a JobExecutionError and a list of NodeEvent and GlobalEvent.
    """

    template_id: Literal["/partials/ErrorAttribution"] = "/partials/ErrorAttribution"
    TEMPLATE = textwrap.dedent(
        """\
        Job execution error:
          {{#job_execution_error}}{{>JobExecutionError}}{{/job_execution_error}}

        Node attributions:
          {{#node_events}}
          {{>NodeEvent}}
          {{/node_events}}
        {{#global_events}}

        Global attributions:
          {{#global_events}}
          {{>GlobalEvent}}
          {{/global_events}}
        {{/global_events}}
        """
    )

    job_execution_error: JobExecutionError
    node_events: list[NodeEvent]
    global_events: list[GlobalEvent] | None = pydantic.Field(default=[])


class ReachedMaxRequeueTime(NotificationTemplate):
    """The job has reached the maximum requeue time.

    Examples:
        >>> message = ReachedMaxRequeueTime(
        ...     job_id="123", job_attempt_index=7, job_name="some-job", message="hello message"
        ... )
        >>> expected_message = (
        ...     "The job 123(7) job name some-job has reached the max requeue time, reason: hello message."
        ... )
        >>> assert_equal(message.render_message(), expected_message)

        >>> assert_equal(message.render_title(), "Max requeue limit reached for job 123")
    """

    template_id: Literal["/templates/ReachedMaxRequeueTime"] = "/templates/ReachedMaxRequeueTime"

    DEFAULT_TYPE = NotificationType.warning

    TITLE_TEMPLATE = "Max requeue limit reached for job {{job_id}}"
    TEMPLATE = "The job {{>JobRepr}} job name {{>JobName}} has reached the max requeue time, reason: {{message}}."

    job_id: str
    job_attempt_index: int
    job_name: str
    message: str


class JobStopped(NotificationTemplate):
    r"""Advice to stop a job attempt has been issued.

    Examples:
        >>> message = JobStopped(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     event_body="this is the event body",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to stop job 456(7) (job name: some-job) issued.
        ... Reason: `SILLY`.
        ... Log: `this is the event body`.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Advice to stop job 456 issued"
        >>> assert_equal(message.render_title(), expected_title)

        The `event_body` is optional:

        >>> message = JobStopped(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to stop job 456(7) (job name: some-job) issued.
        ... Reason: `SILLY`.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)
    """

    template_id: Literal["/templates/JobStopped"] = "/templates/JobStopped"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Advice to stop job {{job_id}} issued"
    TEMPLATE = textwrap.dedent(
        """\
        Advice to stop job {{>JobRepr}} (job name: {{>JobName}}) issued.
        Reason: `{{event_type}}`.
        {{#event_body}}
        Log: `{{event_body}}`.
        {{/event_body}}
        """
    )

    job_id: str
    job_attempt_index: int
    job_name: str
    event_type: str
    event_body: str | None = None
    user: str


class JobRequeued(NotificationTemplate):
    r"""Advice to requeue a job attempt has been issued.

    Examples:
        >>> message = JobRequeued(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     event_body="this is the event body",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to requeue job 456(7) (job name: some-job) issued.
        ... Reason: `SILLY`.
        ... Log: `this is the event body`.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Advice to requeue job 456 issued"
        >>> assert_equal(message.render_title(), expected_title)

        The `event_body` is optional:

        >>> message = JobRequeued(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to requeue job 456(7) (job name: some-job) issued.
        ... Reason: `SILLY`.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)
    """

    template_id: Literal["/templates/JobRequeued"] = "/templates/JobRequeued"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Advice to requeue job {{job_id}} issued"
    TEMPLATE = textwrap.dedent(
        """\
        Advice to requeue job {{>JobRepr}} (job name: {{>JobName}}) issued.
        Reason: `{{event_type}}`.
        {{#event_body}}
        Log: `{{event_body}}`.
        {{/event_body}}
        """
    )

    job_id: str
    job_attempt_index: int
    job_name: str
    event_type: str
    event_body: str | None = None
    user: str


class SuccessfullyFinished(NotificationTemplate):
    r"""The job successfully finished.

    Examples:
        >>> message = SuccessfullyFinished(job_id="123", job_attempt_index=7, job_name="some-job")
        >>> expected_message = "Job 123(7) (some-job) finished."
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Job 123 finished successfully"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/SuccessfullyFinished"] = "/templates/SuccessfullyFinished"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Job {{job_id}} finished successfully"
    TEMPLATE = "Job {{>JobRepr}} ({{>JobName}}) finished."

    job_id: str
    job_attempt_index: int
    job_name: str


class GpuThroughputDropped(NotificationTemplate):
    r"""Show that autonomous recovery engine has found a straggler.

    Examples:
        >>> message = GpuThroughputDropped(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     event_body="this is the event body",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (some-job) `GPU throughput dropped`: `SILLY`.
        ... Log: `this is the event body`
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Performance alert: GPU throughput dropped for job 456"
        >>> assert_equal(message.render_title(), expected_title)

        The `event_body` is optional:

        >>> message = GpuThroughputDropped(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     event_type="SILLY",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (some-job) `GPU throughput dropped`: `SILLY`.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)
    """

    template_id: Literal["/templates/GpuThroughputDropped"] = "/templates/GpuThroughputDropped"

    DEFAULT_TYPE = NotificationType.performance_alert

    TITLE_TEMPLATE = "Performance alert: GPU throughput dropped for job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} ({{>JobName}}) `GPU throughput dropped`: `{{event_type}}`.
        {{#event_body}}
        Log: `{{event_body}}`
        {{/event_body}}
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    event_type: str
    event_body: str | None = None
    user: str


class BadNodesIsolated(NotificationTemplate):
    r"""Advice to exclude nodes from a job attempt has been issued.

    Examples:
        >>> message = BadNodesIsolated(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     bad_nodes={"aaa": ["a1", "a2"], "bbb": ["b1"]},
        ...     non_isolated_nodes={"ccc": ["c1"], "ddd": ["d1", "d2"]},
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to exclude nodes {'aaa': ['a1', 'a2'], 'bbb': ['b1']} from job 456(7) (job name: some-job) issued.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Advice to isolate nodes for job 456 issued"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/BadNodesIsolated"] = "/templates/BadNodesIsolated"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Advice to isolate nodes for job {{job_id}} issued"
    TEMPLATE = textwrap.dedent(
        """\
        Advice to exclude nodes {{bad_nodes}} from job {{>JobRepr}} (job name: {{>JobName}}) issued.
        """
    )

    job_id: str
    job_attempt_index: int
    job_name: str
    bad_nodes: dict[str, list[str]]
    non_isolated_nodes: dict[str, list[str]]
    user: str


class AnomalyAttributionResult(NotificationTemplate):
    r"""
    Anomaly attribution result for job attempt.

    Examples:
        If `summary` and `insight` are provided, skip the detailed error attribution:

            >>> message = AnomalyAttributionResult(
            ...     job_id="12345",
            ...     job_attempt_index=0,
            ...     summary="A summary from job manager.",
            ...     insight="Some insights from job manager.",
            ...     anomaly_type="CRASH",
            ...     error_attributions=[
            ...         ErrorAttribution(
            ...             job_execution_error=JobExecutionError(
            ...                 error_message="random error message",
            ...                 error_signature="random error signature",
            ...             ),
            ...             node_events=[
            ...                 NodeEvent(
            ...                     node="node_0",
            ...                     exclusion_type="temporary",
            ...                     error_attribution="system",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...                 NodeEvent(
            ...                     node="node_1",
            ...                     exclusion_type="confirmed",
            ...                     error_attribution="hardware",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...             ],
            ...         ),
            ...     ],
            ...     user="some-user",
            ... )
            >>> expected_message = '''\
            ... Attribution for the anomaly of type 'CRASH' for job 12345(0).
            ...
            ... Summary:
            ...   A summary from job manager.
            ...
            ... Insight:
            ...   Some insights from job manager.
            ... '''
            >>> assert_equal(message.render_message(), expected_message)

        If `summary` and `insight` are omitted, should display all the attribution info:

            >>> message = AnomalyAttributionResult(
            ...     job_id="12345",
            ...     job_attempt_index=0,
            ...     anomaly_type="CRASH",
            ...     error_attributions=[
            ...         ErrorAttribution(
            ...             job_execution_error=JobExecutionError(
            ...                 error_message="random error message",
            ...                 error_signature="random error signature",
            ...             ),
            ...             node_events=[
            ...                 NodeEvent(
            ...                     node="node_0",
            ...                     exclusion_type="temporary",
            ...                     error_attribution="system",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...                 NodeEvent(
            ...                     node="node_1",
            ...                     exclusion_type="confirmed",
            ...                     error_attribution="hardware",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...             ],
            ...         ),
            ...     ],
            ...     user="some-user",
            ... )
            >>> expected_message = '''\
            ... Attribution for the anomaly of type 'CRASH' for job 12345(0).
            ...
            ... Job execution error:
            ...   Error message: random error message
            ...   Error signature: random error signature
            ...
            ... Node attributions:
            ...   node_0:
            ...     exclusion type 'temporary'
            ...     error attribution 'system'
            ...     error attribution sources: ['APPLICATION_LOG', 'SYSLOG']
            ...   node_1:
            ...     exclusion type 'confirmed'
            ...     error attribution 'hardware'
            ...     error attribution sources: ['APPLICATION_LOG', 'SYSLOG']
            ... '''
            >>> assert_equal(message.render_message(), expected_message)

        The `error_message` can be `None`:

            >>> message_with_none = AnomalyAttributionResult(
            ...     job_id="12345",
            ...     job_attempt_index=0,
            ...     anomaly_type="CRASH",
            ...     error_attributions=[
            ...         ErrorAttribution(
            ...             job_execution_error=JobExecutionError(
            ...                 error_message=None,
            ...                 error_signature="random error signature",
            ...             ),
            ...             node_events=[
            ...                 NodeEvent(
            ...                     node="node_0",
            ...                     exclusion_type="temporary",
            ...                     error_attribution="system",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...                 NodeEvent(
            ...                     node="node_1",
            ...                     exclusion_type="confirmed",
            ...                     error_attribution="hardware",
            ...                     error_attribution_sources=["APPLICATION_LOG", "SYSLOG"]
            ...                 ),
            ...             ],
            ...         ),
            ...     ],
            ...     user="some-user",
            ... )
            >>> expected_message_with_none = '''\
            ... Attribution for the anomaly of type 'CRASH' for job 12345(0).
            ...
            ... Job execution error:
            ...   Error message:
            ...   Error signature: random error signature
            ...
            ... Node attributions:
            ...   node_0:
            ...     exclusion type 'temporary'
            ...     error attribution 'system'
            ...     error attribution sources: ['APPLICATION_LOG', 'SYSLOG']
            ...   node_1:
            ...     exclusion type 'confirmed'
            ...     error attribution 'hardware'
            ...     error attribution sources: ['APPLICATION_LOG', 'SYSLOG']
            ... '''
            >>> assert_equal(message_with_none.render_message(), expected_message_with_none)
    """

    template_id: Literal["/templates/AnomalyAttributionResult"] = "/templates/AnomalyAttributionResult"

    DEFAULT_TYPE = NotificationType.warning

    TITLE_TEMPLATE = "Anomaly attribution for job {{>JobRepr}}"
    TEMPLATE = textwrap.dedent(
        """\
        Attribution for the anomaly of type '{{anomaly_type}}' for job {{>JobRepr}}.
        {{#summary}}

        Summary:
          {{summary}}
        {{/summary}}
        {{#insight}}

        Insight:
          {{insight}}
        {{/insight}}
        {{^summary}}
        {{^insight}}

        {{#error_attributions}}
        {{>ErrorAttribution}}
        {{/error_attributions}}
        {{/insight}}
        {{/summary}}
        """
    )

    job_id: str
    job_attempt_index: int
    anomaly_type: str
    error_attributions: list[ErrorAttribution]
    user: str
    summary: str | None = None
    insight: str | None = None


class ReleasedJob(NotificationTemplate):
    r"""Advice to release a job attempt has been issued.

    Examples:
        >>> message = ReleasedJob(
        ...     job_id="123",
        ...     job_attempt_index=4,
        ...     job_name="some-job",
        ...     user="some-user",
        ... )
        >>> expected_message = '''\
        ... Advice to release job 123(4) (job name: some-job) issued.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Advice to release job 123 issued"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/ReleasedJob"] = "/templates/ReleasedJob"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Advice to release job {{job_id}} issued"
    TEMPLATE = textwrap.dedent(
        """\
        Advice to release job {{>JobRepr}} (job name: {{>JobName}}) issued.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    user: str


class NoMoreReleasingJobs(NotificationTemplate):
    r"""autonomous recovery engine can't release any more jobs.

    Examples:
        >>> message = NoMoreReleasingJobs(job_name="some-job", user="some-user")
        >>> expected_message = '''\
        ... No more jobs in `PENDING(JobHeldUser)` state for job name: some-job - won't release any new jobs.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "No more pending jobs for some-job"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/NoMoreReleasingJobs"] = "/templates/NoMoreReleasingJobs"

    DEFAULT_TYPE = NotificationType.warning

    TITLE_TEMPLATE = "No more pending jobs for {{job_name}}"
    TEMPLATE = textwrap.dedent(
        """\
        No more jobs in `PENDING(JobHeldUser)` state for job name: {{>JobName}} - won't release any new jobs.
        """
    )
    job_name: str
    user: str


class ManualJobStopped(NotificationTemplate):
    r"""A user has manually stopped a job via cockpit.

    Examples:
        >>> message = ManualJobStopped(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     triggerer=User(user="job-killer-guy"),
        ...     user="some-person",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (job name: some-job) has been `STOPPED` manually via Cockpit by job-killer-guy.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Manually stopped job 456"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/ManualJobStopped"] = "/templates/ManualJobStopped"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Manually stopped job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} (job name: {{>JobName}}) has been `STOPPED` manually via Cockpit by {{#triggerer}}{{>User}}{{/triggerer}}.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    triggerer: User
    user: str


class ManualJobRequeued(NotificationTemplate):
    r"""A user has manually requeued a job via cockpit.

    Examples:
        >>> message = ManualJobRequeued(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     triggerer=User(user="job-killer-guy"),
        ...     user="some-person",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (job name: some-job) has been `REQUEUED` manually via Cockpit by job-killer-guy.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Manually requeued job 456"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/ManualJobRequeued"] = "/templates/ManualJobRequeued"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Manually requeued job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} (job name: {{>JobName}}) has been `REQUEUED` manually via Cockpit by {{#triggerer}}{{>User}}{{/triggerer}}.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    triggerer: User
    user: str


class ManualJobPaused(NotificationTemplate):
    r"""A user has manually paused a job via cockpit.

    Examples:
        >>> message = ManualJobPaused(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     triggerer=User(user="job-killer-guy"),
        ...     user="some-person",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (job name: some-job) has been `PAUSED` manually via Cockpit by job-killer-guy.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Manually paused job 456"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/ManualJobPaused"] = "/templates/ManualJobPaused"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Manually paused job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} (job name: {{>JobName}}) has been `PAUSED` manually via Cockpit by {{#triggerer}}{{>User}}{{/triggerer}}.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    triggerer: User
    user: str


class ManualJobStarted(NotificationTemplate):
    r"""A user has manually requeued a job via cockpit.

    Examples:
        >>> message = ManualJobStarted(
        ...     job_id="456",
        ...     job_attempt_index=7,
        ...     job_name="some-job",
        ...     triggerer=User(user="job-killer-guy"),
        ...     user="some-person",
        ... )
        >>> expected_message = '''\
        ... Job 456(7) (job name: some-job) has been `STARTED` manually via Cockpit by job-killer-guy.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Manually started job 456"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/ManualJobStarted"] = "/templates/ManualJobStarted"

    DEFAULT_TYPE = NotificationType.info

    TITLE_TEMPLATE = "Manually started job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} (job name: {{>JobName}}) has been `STARTED` manually via Cockpit by {{#triggerer}}{{>User}}{{/triggerer}}.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    triggerer: User
    user: str


class InvalidJobConfig(NotificationTemplate):
    r"""Notify user that the job config is not compatible with NVIDIA Recovery's requirements.

    Examples:
        >>> message = InvalidJobConfig(
        ...     job_id="456",
        ...     job_attempt_index=0,
        ...     job_name="some-job",
        ...     reason="Invalid job log file name",
        ...     user="some-person",
        ... )
        >>> expected_message = '''\
        ... Job 456(0) (job name: some-job) has invalid configuration that's not compatible with NVIDIA Recovery's requirements.
        ... Reason: Invalid job log file name.
        ... '''
        >>> assert_equal(message.render_message(), expected_message)

        >>> expected_title = "Invalid configuration for job 456"
        >>> assert_equal(message.render_title(), expected_title)
    """

    template_id: Literal["/templates/InvalidJobConfig"] = "/templates/InvalidJobConfig"

    TITLE_TEMPLATE = "Invalid configuration for job {{job_id}}"
    TEMPLATE = textwrap.dedent(
        """\
        Job {{>JobRepr}} (job name: {{>JobName}}) has invalid configuration that's not compatible with NVIDIA Recovery's requirements.
        Reason: {{reason}}.
        """
    )
    job_id: str
    job_attempt_index: int
    job_name: str
    reason: str
    user: str


_base_messages = {
    subclass.__annotations__["template_id"]: subclass for subclass in NotificationTemplate.__subclasses__()
}


# Use RootModel to make sure this object gets its own JSON Schema definition.
# (If we use the Union as a type hint normally, then the schema gets defined under the relevant property's schema,
# rather than its own `$ref`-able schema.)
class TemplateData(pydantic.RootModel):
    """A template_id and the data associated with it."""

    # If we don't set `json_schema_mode_override`, TemplateData will be split into two different (but identical!)
    # schemas: TemplateData-Input and TemplateData-Output.
    # This is silly, and we don't want it.
    model_config = pydantic.ConfigDict(
        **BaseModel.model_config,
        json_schema_mode_override="serialization",
    )

    root: Annotated[
        JobRepr
        | JobName
        | User
        | JobExecutionError
        | NodeEvent
        | GlobalEvent
        | ErrorAttribution
        | ReachedMaxRequeueTime
        | InvalidJobConfig
        | JobStopped
        | JobRequeued
        | SuccessfullyFinished
        | GpuThroughputDropped
        | BadNodesIsolated
        | AnomalyAttributionResult
        | ReleasedJob
        | NoMoreReleasingJobs
        | ManualJobStopped
        | ManualJobRequeued
        | ManualJobPaused
        | ManualJobStarted,
        pydantic.Field(discriminator="template_id"),
    ]

    def render_message(self, *args, **kwargs):
        return self.root.render_message(*args, **kwargs)

    def render_title(self, *args, **kwargs):
        return self.root.render_title(*args, **kwargs)
