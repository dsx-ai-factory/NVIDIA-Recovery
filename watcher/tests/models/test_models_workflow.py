# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from pydantic import ValidationError
from watcher.models.workflow import (
    WorkflowStep,
    WorkflowStepConfigAction,
    WorkflowStepNooutputConfig,
    WorkflowStepNooutputConfigAttributes,
    WorkflowStepRegexConfig,
    WorkflowStepRegexConfigAttributes,
    WorkflowStepRegexConfigAttributesAdvancedRegex,
    WorkflowStepStragglerConfig,
    WorkflowStepStragglerConfigAttributes,
)


## WorkflowStepRegexConfigAttributesAdvancedRegex
##
def test_workflow_step_regex_parser_advanced_valid_data(workflow_step_regex_parser_advanced_config_dict):
    config = WorkflowStepRegexConfigAttributesAdvancedRegex(
        **workflow_step_regex_parser_advanced_config_dict["attributes"]["advanced_regex"][0]
    )
    assert config.event_type == "NESTED_RESTARTER"
    assert len(config.regexes) == 2
    assert config.regexes[0] == "^.*NestedRestarter.*state\\=(?P<state>[a-zA-Z]+)\\s+stage\\=(?P<stage>[a-zA-Z]+).*$"


def test_workflow_step_regex_parser_advanced_empty_data():
    config = WorkflowStepRegexConfigAttributesAdvancedRegex()
    assert config.event_type is None
    assert config.regexes is None


def test_workflow_step_regex_parser_advanced_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        WorkflowStepRegexConfigAttributesAdvancedRegex(
            event_type="NESTED_RESTARTER",
            regexes=["^.*NestedRestarter.*state\\=(?P<state>[a-zA-Z]+)\\s+stage\\=(?P<stage>[a-zA-Z]+).*$"],
            extra_field="This should cause an error",
        )


## WorkflowStepRegexConfigAttributes
##
def test_workflow_step_regex_parser_attributes_valid_data(workflow_step_regex_parser_advanced_config_dict):
    config = WorkflowStepRegexConfigAttributes(**workflow_step_regex_parser_advanced_config_dict["attributes"])
    assert config.regex is None
    assert config.advanced_regex is not None
    assert config.publish_events is True
    assert config.stop_after_first_match is True


def test_workflow_step_regex_parser_attributes_dedup_defaults():
    config = WorkflowStepRegexConfigAttributes(regex=".*")
    assert config.dedup is False
    assert config.dedup_signature_pattern is None
    assert config.dedup_strip_patterns is None


def test_workflow_step_regex_parser_attributes_dedup_configured():
    config = WorkflowStepRegexConfigAttributes(
        regex=".*",
        dedup=True,
        dedup_signature_pattern=r"body=(.+)",
        dedup_strip_patterns=[r"timestamp=\d+", r"uuid=\S+"],
    )
    assert config.dedup is True
    assert config.dedup_signature_pattern == r"body=(.+)"
    assert config.dedup_strip_patterns == [r"timestamp=\d+", r"uuid=\S+"]


def test_workflow_step_regex_parser_attributes_invalid_dedup_signature_pattern():
    with pytest.raises(ValueError, match="Invalid dedup_signature_pattern"):
        WorkflowStepRegexConfigAttributes(regex=".*", dedup_signature_pattern=r"[invalid(")


def test_workflow_step_regex_parser_attributes_invalid_dedup_strip_pattern():
    with pytest.raises(ValueError, match="Invalid dedup_strip_pattern"):
        WorkflowStepRegexConfigAttributes(regex=".*", dedup_strip_patterns=[r"valid", r"*invalid"])


def test_workflow_step_regex_parser_attributes_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        WorkflowStepRegexConfigAttributes(
            regex="^.*NestedRestarter.*state\\=(?P<state>[a-zA-Z]+)\\s+stage\\=(?P<stage>[a-zA-Z]+).*$",
            extra_field="This should cause an error",
        )


def test_workflow_step_regex_parser_attributes_validation_missing_events():
    attributes = WorkflowStepRegexConfigAttributes()
    values = {"advanced_regex_parser": True, "events": []}

    with pytest.raises(ValueError) as exc_info:
        attributes.validate_advanced_parser_requirements(values)

    assert "events must be provided" in str(exc_info.value)


def test_workflow_step_regex_parser_attributes_missing_regexes():
    attributes = WorkflowStepRegexConfigAttributes()
    values = {"advanced_regex_parser": True, "events": [{"event_type": "NESTED_RESTARTER", "regexes": []}]}

    with pytest.raises(ValueError) as exc_info:
        attributes.validate_advanced_parser_requirements(values)

    assert "events:regexes must be provided" in str(exc_info.value)


def test_workflow_step_regex_parser_attributes_invalid_regex():
    attributes = WorkflowStepRegexConfigAttributes()
    values = {
        "advanced_regex_parser": True,
        "events": [{"event_type": "NESTED_RESTARTER", "regexes": ["*invalid regex("]}],
    }

    with pytest.raises(ValueError) as exc_info:
        attributes.validate_advanced_parser_requirements(values)

    assert "Invalid regex pattern" in str(exc_info.value)


## WorkflowStepRegexConfig
##
def test_workflow_step_regex_config(workflow_step_regex_parser_advanced_config_dict):
    config = WorkflowStepRegexConfig(**workflow_step_regex_parser_advanced_config_dict)
    assert config.name == "nested_restarter_regex_parse"
    assert config.attributes.advanced_regex is not None
    assert len(config.attributes.advanced_regex) == 1
    assert len(config.attributes.advanced_regex[0].regexes) == 2
    assert config.attributes.publish_events is True
    assert config.attributes.stop_after_first_match is True


def test_workflow_step_regex_config_inheritance_from_workflow_step(workflow_step_regex_parser_advanced_config_dict):
    config = WorkflowStepRegexConfig(**workflow_step_regex_parser_advanced_config_dict)
    assert isinstance(config, WorkflowStep)


def test_workflow_step_regex_config_missing_attributes():
    with pytest.raises(ValidationError):
        WorkflowStepRegexConfig(name="Missing Attributes Test", type="regex_parser")


## Test fixtures for WorkflowStepStragglerConfig
##
@pytest.fixture
def workflow_step_straggler_config_action_dict():
    return {"send_message_eventbus": "STRAGGLER"}


@pytest.fixture
def workflow_step_straggler_config_action_with_unique_dict():
    return {"send_message_eventbus": "STRAGGLER", "unique": True}


@pytest.fixture
def workflow_step_straggler_config_attributes_dict(workflow_step_straggler_config_action_dict):
    return {
        "cache_key": "tflop_straggler",
        "window_size": 100,
        "base_percentile_threshold": 35.0,
        "anomaly_percent_threshold": 10.0,
        "alert_smoothing_count": 5,
        "warmup_grace_period_secs": 600,
        "min_window_size_for_detection": 10,
        "min_interval_between_datapoints_secs": 30,
        "skip_too_close_datapoints": False,
        "reset_after_good_values": 5,
        "notify_once": False,
        "action": workflow_step_straggler_config_action_dict,
    }


@pytest.fixture
def workflow_step_straggler_config_attributes_with_notify_once_false_dict(workflow_step_straggler_config_action_dict):
    return {
        "cache_key": "tflop_straggler",
        "window_size": 100,
        "base_percentile_threshold": 35.0,
        "anomaly_percent_threshold": 10.0,
        "alert_smoothing_count": 5,
        "warmup_grace_period_secs": 600,
        "min_window_size_for_detection": 10,
        "min_interval_between_datapoints_secs": 30,
        "skip_too_close_datapoints": False,
        "reset_after_good_values": 5,
        "notify_once": False,
        "action": workflow_step_straggler_config_action_dict,
    }


@pytest.fixture
def workflow_step_straggler_config_dict(workflow_step_straggler_config_attributes_dict):
    return {
        "name": "detect_straggler",
        "type": "moving_avg",
        "attributes": workflow_step_straggler_config_attributes_dict,
    }


## WorkflowStepConfigAction
##
def test_workflow_step_config_action_valid_data(workflow_step_straggler_config_action_dict):
    action = WorkflowStepConfigAction(**workflow_step_straggler_config_action_dict)
    assert action.send_message_eventbus == "STRAGGLER"
    assert action.unique is None


def test_workflow_step_config_action_with_unique_true():
    action_dict = {"send_message_eventbus": "STRAGGLER", "unique": True}
    action = WorkflowStepConfigAction(**action_dict)
    assert action.send_message_eventbus == "STRAGGLER"
    assert action.unique is True


def test_workflow_step_config_action_with_unique_false():
    action_dict = {"send_message_eventbus": "CHECKPOINTLOAD", "unique": False}
    action = WorkflowStepConfigAction(**action_dict)
    assert action.send_message_eventbus == "CHECKPOINTLOAD"
    assert action.unique is False


def test_workflow_step_config_action_empty_data():
    with pytest.raises(ValidationError):
        WorkflowStepConfigAction()


def test_workflow_step_config_action_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        WorkflowStepConfigAction(send_message_eventbus="STRAGGLER", extra_field="This should cause an error")


## WorkflowStepStragglerConfigAttributes
##
def test_workflow_step_straggler_config_attributes_valid_data(workflow_step_straggler_config_attributes_dict):
    attributes = WorkflowStepStragglerConfigAttributes(**workflow_step_straggler_config_attributes_dict)
    assert attributes.cache_key == "tflop_straggler"
    assert attributes.window_size == 100
    assert attributes.base_percentile_threshold == 35.0
    assert attributes.anomaly_percent_threshold == 10.0
    assert attributes.alert_smoothing_count == 5
    assert attributes.warmup_grace_period_secs == 600
    assert attributes.min_window_size_for_detection == 10
    assert attributes.min_interval_between_datapoints_secs == 30
    assert attributes.skip_too_close_datapoints is False
    assert attributes.reset_after_good_values == 5
    assert attributes.notify_once is False
    assert attributes.action.send_message_eventbus == "STRAGGLER"


def test_workflow_step_straggler_config_attributes_missing_required_fields():
    with pytest.raises(ValidationError):
        WorkflowStepStragglerConfigAttributes()


def test_workflow_step_straggler_config_attributes_with_unique_action(
    workflow_step_straggler_config_action_with_unique_dict,
):
    attributes_dict = {
        "cache_key": "tflop_straggler",
        "window_size": 100,
        "base_percentile_threshold": 35.0,
        "anomaly_percent_threshold": 10.0,
        "alert_smoothing_count": 5,
        "warmup_grace_period_secs": 600,
        "min_window_size_for_detection": 10,
        "min_interval_between_datapoints_secs": 30,
        "skip_too_close_datapoints": False,
        "reset_after_good_values": 5,
        "action": workflow_step_straggler_config_action_with_unique_dict,
    }
    attributes = WorkflowStepStragglerConfigAttributes(**attributes_dict)
    assert attributes.action.send_message_eventbus == "STRAGGLER"
    assert attributes.action.unique is True
    assert attributes.notify_once is False  # Default value


def test_workflow_step_straggler_config_attributes_with_notify_once_false(
    workflow_step_straggler_config_attributes_with_notify_once_false_dict,
):
    attributes = WorkflowStepStragglerConfigAttributes(
        **workflow_step_straggler_config_attributes_with_notify_once_false_dict
    )
    assert attributes.cache_key == "tflop_straggler"
    assert attributes.notify_once is False
    assert attributes.action.send_message_eventbus == "STRAGGLER"


def test_workflow_step_straggler_config_attributes_default_notify_once():
    # Test that notify_once defaults to False when not specified
    attributes_dict = {
        "cache_key": "tflop_straggler",
        "window_size": 100,
        "base_percentile_threshold": 35.0,
        "anomaly_percent_threshold": 10.0,
        "alert_smoothing_count": 5,
        "warmup_grace_period_secs": 600,
        "min_window_size_for_detection": 10,
        "min_interval_between_datapoints_secs": 30,
        "skip_too_close_datapoints": False,
        "reset_after_good_values": 5,
        "action": {"send_message_eventbus": "STRAGGLER"},
        # notify_once not specified - should use default
    }
    attributes = WorkflowStepStragglerConfigAttributes(**attributes_dict)
    assert attributes.notify_once is False


def test_workflow_step_straggler_config_attributes_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        WorkflowStepStragglerConfigAttributes(
            cache_key="tflop_straggler",
            window_size=100,
            base_percentile_threshold=35.0,
            anomaly_percent_threshold=10.0,
            alert_smoothing_count=5,
            warmup_grace_period_secs=600,
            min_window_size_for_detection=10,
            min_interval_between_datapoints_secs=30,
            skip_too_close_datapoints=False,
            reset_after_good_values=5,
            action={"send_message_eventbus": "STRAGGLER"},
            extra_field="This should cause an error",
        )


## WorkflowStepStragglerConfig
##
def test_workflow_step_straggler_config_valid_data(workflow_step_straggler_config_dict):
    config = WorkflowStepStragglerConfig(**workflow_step_straggler_config_dict)
    assert config.name == "detect_straggler"
    assert config.type == "moving_avg"
    assert config.attributes.cache_key == "tflop_straggler"
    assert config.attributes.window_size == 100
    assert config.attributes.action.send_message_eventbus == "STRAGGLER"


def test_workflow_step_straggler_config_inheritance_from_workflow_step(workflow_step_straggler_config_dict):
    config = WorkflowStepStragglerConfig(**workflow_step_straggler_config_dict)
    assert isinstance(config, WorkflowStep)


def test_workflow_step_straggler_config_missing_attributes():
    with pytest.raises(ValidationError):
        WorkflowStepStragglerConfig(name="Missing Attributes Test", type="moving_avg")


## Test fixtures for WorkflowStepNooutputConfig
##
@pytest.fixture
def workflow_step_nooutput_config_attributes_dict():
    return {
        "cache_key": "hang_nooutput",
        "query": {
            "regex": [
                {"PRE_TRAINING": "type=(start|event).*name=main_function"},
                {"TRAINING_LOOP": "event.name=training_iterations"},
            ],
        },
        "interval": {"PRE_TRAINING": 960, "TRAINING_LOOP": 840},
        "last_iteration_enabled": True,
        "last_iteration_regex": r"iteration\s*(\d+)\s*\/\s*(\d+)\s*\|",
        "last_iteration_operation_schema": "{current} > 0.99 * {total}",
        "false_positive_scan_cache_key": "false_positive",
        "false_positive_scan_timer": 600,
        "action": {"send_message_eventbus": "HANGNOOUTPUT"},
    }


@pytest.fixture
def workflow_step_nooutput_config_dict(workflow_step_nooutput_config_attributes_dict):
    return {
        "name": "noprogress",
        "type": "nooutput",
        "bucket_by": ["job_id", "log_file_name"],
        "attributes": workflow_step_nooutput_config_attributes_dict,
    }


## WorkflowStepNooutputConfigAttributes
##
def test_workflow_step_nooutput_config_attributes_valid_data(workflow_step_nooutput_config_attributes_dict):
    attrs = WorkflowStepNooutputConfigAttributes(**workflow_step_nooutput_config_attributes_dict)
    assert attrs.cache_key == "hang_nooutput"
    assert attrs.interval == {"PRE_TRAINING": 960, "TRAINING_LOOP": 840}
    assert attrs.last_iteration_enabled is True
    assert attrs.false_positive_scan_timer == 600
    assert attrs.action.send_message_eventbus == "HANGNOOUTPUT"


def test_workflow_step_nooutput_config_attributes_defaults():
    attrs = WorkflowStepNooutputConfigAttributes(
        cache_key="k",
        query={"regex": []},
        interval={"TRAINING_LOOP": 60},
        action={"send_message_eventbus": "X"},
    )
    assert attrs.last_iteration_enabled is False
    assert attrs.last_iteration_regex is None
    assert attrs.last_iteration_operation_schema is None
    assert attrs.false_positive_scan_cache_key is None
    assert attrs.false_positive_scan_timer is None


def test_workflow_step_nooutput_config_attributes_missing_required_fields():
    with pytest.raises(ValidationError):
        WorkflowStepNooutputConfigAttributes()


def test_workflow_step_nooutput_config_attributes_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        WorkflowStepNooutputConfigAttributes(
            cache_key="k",
            query={"regex": []},
            interval={"TRAINING_LOOP": 60},
            action={"send_message_eventbus": "X"},
            extra_field="bad",
        )


## WorkflowStepNooutputConfig
##
def test_workflow_step_nooutput_config_valid_data(workflow_step_nooutput_config_dict):
    config = WorkflowStepNooutputConfig(**workflow_step_nooutput_config_dict)
    assert config.name == "noprogress"
    assert config.type == "nooutput"
    assert config.attributes.cache_key == "hang_nooutput"
    assert config.attributes.action.send_message_eventbus == "HANGNOOUTPUT"
    assert config.bucket_by == ["job_id", "log_file_name"]


def test_workflow_step_nooutput_config_inheritance_from_workflow_step(workflow_step_nooutput_config_dict):
    config = WorkflowStepNooutputConfig(**workflow_step_nooutput_config_dict)
    assert isinstance(config, WorkflowStep)


def test_workflow_step_nooutput_config_missing_attributes():
    with pytest.raises(ValidationError):
        WorkflowStepNooutputConfig(name="Missing Attributes Test", type="nooutput")
