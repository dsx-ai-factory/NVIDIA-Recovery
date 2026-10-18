# AnomalyIsolationResult

Anomaly attribution result for job attempt.  Examples:     >>> message = AnomalyIsolationResult(     ...     job_id=\"12345\",     ...     job_attempt_index=0,     ...     anomaly_type=\"CRASH\",     ...     job_execution_error=JobExecutionError(     ...         error_message=\"random error message\",     ...         error_signature=\"random error signature\",     ...     ),     ...     node_events=[     ...         NodeEvent(     ...             node=\"node_0\",     ...             exclusion_type=\"temporary\",     ...             error_attribution=\"system\",     ...             error_attribution_sources=[\"APPLICATION_LOG\", \"SYSLOG\"]     ...         ),     ...         NodeEvent(     ...             node=\"node_1\",     ...             exclusion_type=\"confirmed\",     ...             error_attribution=\"hardware\",     ...             error_attribution_sources=[\"APPLICATION_LOG\", \"SYSLOG\"]     ...         ),     ...     ],     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... NVIDIA Recovery has calculated the attribution for the anomaly of type 'CRASH' for job 12345(0).     ...     ... Error message: random error message     ... Error signature: random error signature     ...     ... Node attributions:     ... node_0:     ...    exclusion type 'temporary'     ...    error attribution 'system'     ...    error attribution sources: ['APPLICATION_LOG', 'SYSLOG']     ... node_1:     ...    exclusion type 'confirmed'     ...    error attribution 'hardware'     ...    error attribution sources: ['APPLICATION_LOG', 'SYSLOG']     ...     ... some-user     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/AnomalyIsolationResult']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**anomaly_type** | **str** |  |
**job_execution_error** | [**JobExecutionError**](JobExecutionError.md) |  |
**node_events** | [**List[NodeEvent]**](NodeEvent.md) |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.anomaly_isolation_result import AnomalyIsolationResult

# TODO update the JSON string below
json = "{}"
# create an instance of AnomalyIsolationResult from a JSON string
anomaly_isolation_result_instance = AnomalyIsolationResult.from_json(json)
# print the JSON string representation of the object
print(AnomalyIsolationResult.to_json())

# convert the object into a dict
anomaly_isolation_result_dict = anomaly_isolation_result_instance.to_dict()
# create an instance of AnomalyIsolationResult from a dict
anomaly_isolation_result_from_dict = AnomalyIsolationResult.from_dict(anomaly_isolation_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
