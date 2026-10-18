# InvalidJobConfig

Notify user that the job config is not compatible with NVIDIA Recovery's requirements.  Examples:     >>> message = InvalidJobConfig(     ...     job_id=\"456\",     ...     job_attempt_index=0,     ...     job_name=\"some-job\",     ...     reason=\"Invalid job log file name\",     ...     user=\"some-person\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(0) (job name: some-job) has invalid configuration that's not compatible with NVIDIA Recovery's requirements.     ... Reason: Invalid job log file name.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Invalid configuration for job 456\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/InvalidJobConfig']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**reason** | **str** |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.invalid_job_config import InvalidJobConfig

# TODO update the JSON string below
json = "{}"
# create an instance of InvalidJobConfig from a JSON string
invalid_job_config_instance = InvalidJobConfig.from_json(json)
# print the JSON string representation of the object
print(InvalidJobConfig.to_json())

# convert the object into a dict
invalid_job_config_dict = invalid_job_config_instance.to_dict()
# create an instance of InvalidJobConfig from a dict
invalid_job_config_from_dict = InvalidJobConfig.from_dict(invalid_job_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
