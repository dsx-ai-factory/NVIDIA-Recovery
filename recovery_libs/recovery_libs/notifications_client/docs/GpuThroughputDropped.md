# GpuThroughputDropped

Show that autonomous recovery engine has found a straggler.  Examples:     >>> message = GpuThroughputDropped(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     event_body=\"this is the event body\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(7) (some-job) `GPU throughput dropped`: `SILLY`.     ... Log: `this is the event body`     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"GPU throughput dropped for job 456\"     >>> assert_equal(message.render_title(), expected_title)      The `event_body` is optional:      >>> message = GpuThroughputDropped(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(7) (some-job) `GPU throughput dropped`: `SILLY`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/GpuThroughputDropped']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  | [optional]
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.gpu_throughput_dropped import GpuThroughputDropped

# TODO update the JSON string below
json = "{}"
# create an instance of GpuThroughputDropped from a JSON string
gpu_throughput_dropped_instance = GpuThroughputDropped.from_json(json)
# print the JSON string representation of the object
print(GpuThroughputDropped.to_json())

# convert the object into a dict
gpu_throughput_dropped_dict = gpu_throughput_dropped_instance.to_dict()
# create an instance of GpuThroughputDropped from a dict
gpu_throughput_dropped_from_dict = GpuThroughputDropped.from_dict(gpu_throughput_dropped_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
