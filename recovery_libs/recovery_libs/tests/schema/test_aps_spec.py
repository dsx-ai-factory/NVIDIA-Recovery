# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest

from pydantic import ValidationError
from recovery_libs.schema.aps_spec.spec import (
    DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_PRETRAINING_SECS,
    DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_TRAININGLOOP_SECS,
    AnomalyDetectionConfig,
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
    DagActionEnum,
    DagDependencyStep,
    DagTriggerConditionEnum,
    JobReferenceById,
    JobReferenceByName,
)


class TestApsSlurmJobSpec(unittest.TestCase):
    def test_default_values(self):
        """Test that default values are correctly set"""
        spec = ApsSlurmJobSpec()
        self.assertIsNone(spec.workflow_id)
        self.assertIsNone(spec.workflow_name)
        self.assertIsNone(spec.environment)
        self.assertIsNone(spec.auto_resume_mode)
        self.assertIsNone(spec.max_requeue_times)
        self.assertIsNone(spec.requeue_on_completion)
        self.assertFalse(spec.log_file_paths_shared_across_attempts)
        self.assertEqual(spec.nvrx, "disabled")
        self.assertIsInstance(spec.anomaly_detection_config, AnomalyDetectionConfig)
        self.assertTrue(spec.anomaly_detection_config.log_based_crash_enabled)

    def test_workflow_identity_fields_round_trip_verbatim(self):
        """Test that the canonical workflow ID and deprecated alias remain comment-compatible."""
        workflow_id = "  workflow/id:e\N{COMBINING ACUTE ACCENT}?value=#fragment  "
        workflow_name = "legacy/alias"

        spec = ApsSlurmJobSpec(workflow_id=workflow_id, workflow_name=workflow_name)
        restored = ApsSlurmJobSpec.model_validate_json(spec.model_dump_json())

        self.assertEqual(restored.workflow_id, workflow_id)
        self.assertEqual(restored.workflow_name, workflow_name)

    def test_workflow_identity_fields_do_not_reject_comment_overrides_by_length(self):
        """Optional comment identity validation belongs to the scheduler resolver."""
        oversized_multibyte_id = "\U0001f680" * 257

        spec = ApsSlurmJobSpec(workflow_id=oversized_multibyte_id, workflow_name="")

        self.assertEqual(spec.workflow_id, oversized_multibyte_id)
        self.assertEqual(spec.workflow_name, "")

    def test_workflow_name_json_schema_is_deprecated(self):
        """The emitted APS contract identifies workflow_name as the compatibility alias."""
        properties = ApsSlurmJobSpec.model_json_schema()["properties"]

        self.assertNotIn("deprecated", properties["workflow_id"])
        self.assertTrue(properties["workflow_name"]["deprecated"])

    def test_anomaly_detection_config_log_based_crash_enabled_true(self):
        """Test anomaly_detection_config can have log_based_crash_enabled set to True"""
        spec = ApsSlurmJobSpec(anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=True))
        self.assertIsNotNone(spec.anomaly_detection_config)
        self.assertTrue(spec.anomaly_detection_config.log_based_crash_enabled)

    def test_anomaly_detection_config_log_based_crash_enabled_false(self):
        """Test anomaly_detection_config can have log_based_crash_enabled set to False"""
        spec = ApsSlurmJobSpec(anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False))
        self.assertIsNotNone(spec.anomaly_detection_config)
        self.assertFalse(spec.anomaly_detection_config.log_based_crash_enabled)

    def test_anomaly_detection_config_default_instance(self):
        """Test anomaly_detection_config defaults to AnomalyDetectionConfig() with defaults on ApsSlurmJobSpec"""
        spec = ApsSlurmJobSpec()
        self.assertIsInstance(spec.anomaly_detection_config, AnomalyDetectionConfig)
        self.assertTrue(spec.anomaly_detection_config.log_based_crash_enabled)

    def test_nvrx_validation_valid_values(self):
        """Test that nvrx accepts valid values"""
        valid_values = ["enabled", "disabled", "true", "false"]
        for value in valid_values:
            spec = ApsSlurmJobSpec(nvrx=value)
            self.assertEqual(spec.nvrx, value)

    def test_nvrx_validation_invalid_value(self):
        """Test that nvrx rejects invalid values"""
        with self.assertRaises(ValidationError) as context:
            ApsSlurmJobSpec(nvrx="invalid")
        self.assertIn("nvrx must be one of", str(context.exception))

    def test_complete_spec(self):
        """Test creating a complete spec with all fields"""
        spec = ApsSlurmJobSpec(
            workflow_name="test_workflow",
            environment="prod",
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
            max_requeue_times=5,
            requeue_on_completion=True,
            log_file_paths_shared_across_attempts=True,
            nvrx="enabled",
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False),
        )
        self.assertEqual(spec.workflow_name, "test_workflow")
        self.assertEqual(spec.environment, "prod")
        self.assertEqual(spec.auto_resume_mode, AutoResumeModeEnum.REQUEUE)
        self.assertEqual(spec.max_requeue_times, 5)
        self.assertTrue(spec.requeue_on_completion)
        self.assertTrue(spec.log_file_paths_shared_across_attempts)
        self.assertEqual(spec.nvrx, "enabled")
        self.assertIsNotNone(spec.anomaly_detection_config)
        self.assertFalse(spec.anomaly_detection_config.log_based_crash_enabled)

    def test_serialization_deserialization(self):
        """Test that spec can be serialized and deserialized"""
        spec = ApsSlurmJobSpec(
            workflow_name="test",
            anomaly_detection_config=AnomalyDetectionConfig(log_based_crash_enabled=False),
        )
        spec_dict = spec.model_dump()
        restored_spec = ApsSlurmJobSpec.model_validate(spec_dict)
        self.assertEqual(spec, restored_spec)
        self.assertIsNotNone(restored_spec.anomaly_detection_config)
        self.assertFalse(restored_spec.anomaly_detection_config.log_based_crash_enabled)

    def test_deserialization_ignores_removed_kso_spec(self):
        """Test that old persisted data containing kso_spec can still be deserialized after KSO removal.

        The ApsSlurmJobSpec is serialized and stored in the database. Old records may contain a kso_spec
        field. After removing KSO support, deserialization must not fail when encountering this legacy field.
        """
        legacy_data = {
            "workflow_name": "test_workflow",
            "environment": "prod",
            "kso_spec": {
                "enabled": True,
                "unit": "gpu",
                "target": 100,
                "limit": 80,
                "step_size": 4,
                "operator": "minus",
            },
            "auto_resume_mode": "requeue",
        }
        spec = ApsSlurmJobSpec.model_validate(legacy_data)
        self.assertEqual(spec.workflow_name, "test_workflow")
        self.assertEqual(spec.environment, "prod")
        self.assertEqual(spec.auto_resume_mode, AutoResumeModeEnum.REQUEUE)
        self.assertFalse(hasattr(spec, "kso_spec"))

    def test_deserialization_ignores_removed_kso_spec_json(self):
        """Test that JSON-serialized old data with kso_spec can be deserialized."""

        legacy_json = json.dumps(
            {
                "workflow_name": "test",
                "kso_spec": {
                    "enabled": False,
                    "unit": "node",
                    "target": 256,
                    "limit": 32,
                    "step_size": 8,
                    "operator": "minus",
                },
            }
        )
        spec = ApsSlurmJobSpec.model_validate_json(legacy_json)
        self.assertEqual(spec.workflow_name, "test")
        self.assertFalse(hasattr(spec, "kso_spec"))


class TestAnomalyDetectionConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = AnomalyDetectionConfig()
        self.assertTrue(cfg.log_based_crash_enabled and cfg.log_based_timeout_enabled)
        self.assertEqual(
            cfg.log_based_timeout_threshold_pretraining, DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_PRETRAINING_SECS
        )
        self.assertEqual(
            cfg.log_based_timeout_threshold_trainingloop, DEFAULT_LOG_BASED_TIMEOUT_THRESHOLD_TRAININGLOOP_SECS
        )

    def test_overrides_round_trip(self):
        cfg = AnomalyDetectionConfig(
            log_based_timeout_enabled=False,
            log_based_timeout_threshold_pretraining=1500,
            log_based_timeout_threshold_trainingloop=1200,
        )
        restored = ApsSlurmJobSpec.model_validate(ApsSlurmJobSpec(anomaly_detection_config=cfg).model_dump())
        self.assertEqual(restored.anomaly_detection_config, cfg)

    def test_thresholds_must_be_strict_positive_int(self):
        for field in ("log_based_timeout_threshold_pretraining", "log_based_timeout_threshold_trainingloop"):
            for invalid in (0, -1, True, "1"):
                with self.assertRaises(ValidationError):
                    AnomalyDetectionConfig(**{field: invalid})


class TestDagDependencySpec(unittest.TestCase):
    def test_dag_dependency_step_by_name(self):
        """Test DagDependencyStep with a name-based job reference."""
        step = DagDependencyStep(target=JobReferenceByName(name="preprocessing_job"))
        self.assertEqual(step.target.type, "name")
        self.assertEqual(step.target.name, "preprocessing_job")
        self.assertEqual(step.trigger, DagTriggerConditionEnum.COMPLETED)

    def test_dag_dependency_step_by_id(self):
        """Test DagDependencyStep with an id-based job reference."""
        step = DagDependencyStep(target=JobReferenceById(id="12345"))
        self.assertEqual(step.target.type, "id")
        self.assertEqual(step.target.id, "12345")
        self.assertEqual(step.trigger, DagTriggerConditionEnum.COMPLETED)

    def test_dag_dependency_step_custom_trigger(self):
        """Test DagDependencyStep with an explicitly set trigger condition."""
        step = DagDependencyStep(
            target=JobReferenceByName(name="parent_job"),
            trigger=DagTriggerConditionEnum.COMPLETED,
        )
        self.assertEqual(step.trigger, DagTriggerConditionEnum.COMPLETED)

    def test_dag_dependency_step_from_dict_by_name(self):
        """Test DagDependencyStep deserialization from dict with name reference."""
        data = {"target": {"type": "name", "name": "preprocess"}, "trigger": "completed"}
        step = DagDependencyStep.model_validate(data)
        self.assertEqual(step.target.type, "name")
        self.assertEqual(step.target.name, "preprocess")

    def test_dag_dependency_step_from_dict_by_id(self):
        """Test DagDependencyStep deserialization from dict with id reference."""
        data = {"target": {"type": "id", "id": "99999"}, "trigger": "completed"}
        step = DagDependencyStep.model_validate(data)
        self.assertEqual(step.target.type, "id")
        self.assertEqual(step.target.id, "99999")

    def test_aps_spec_with_dag_dependencies(self):
        """Test ApsSlurmJobSpec with dag_dependencies and dag_action."""
        spec = ApsSlurmJobSpec(
            dag_dependencies=[
                DagDependencyStep(target=JobReferenceByName(name="preprocess")),
                DagDependencyStep(target=JobReferenceById(id="99999")),
            ],
            dag_action=DagActionEnum.RELEASE,
        )
        self.assertEqual(len(spec.dag_dependencies), 2)
        self.assertEqual(spec.dag_action, DagActionEnum.RELEASE)

    def test_aps_spec_dag_defaults_to_none(self):
        """Test that dag fields default to None."""
        spec = ApsSlurmJobSpec()
        self.assertIsNone(spec.dag_dependencies)
        self.assertIsNone(spec.dag_action)

    def test_dag_serialization_deserialization(self):
        """Test round-trip serialization of spec with DAG dependencies."""
        spec = ApsSlurmJobSpec(
            dag_dependencies=[
                DagDependencyStep(target=JobReferenceByName(name="preprocess")),
                DagDependencyStep(target=JobReferenceById(id="55555")),
            ],
            dag_action=DagActionEnum.RELEASE,
        )
        spec_dict = spec.model_dump()
        restored = ApsSlurmJobSpec.model_validate(spec_dict)
        self.assertEqual(len(restored.dag_dependencies), 2)
        self.assertEqual(restored.dag_dependencies[0].target.type, "name")
        self.assertEqual(restored.dag_dependencies[0].target.name, "preprocess")
        self.assertEqual(restored.dag_dependencies[1].target.type, "id")
        self.assertEqual(restored.dag_dependencies[1].target.id, "55555")
        self.assertEqual(restored.dag_action, DagActionEnum.RELEASE)


if __name__ == "__main__":
    unittest.main()
