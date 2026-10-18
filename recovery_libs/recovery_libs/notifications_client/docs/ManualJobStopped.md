# ManualJobStopped

A user has manually stopped a job via cockpit.  Examples:     >>> message = ManualJobStopped(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     triggerer=User(user=\"job-killer-guy\"),     ...     user=\"some-person\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(7) (job name: some-job) has been `STOPPED` manually via Cockpit by job-killer-guy.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Manually stopped job 456\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobStopped']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**triggerer** | [**User**](User.md) |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.manual_job_stopped import ManualJobStopped

# TODO update the JSON string below
json = "{}"
# create an instance of ManualJobStopped from a JSON string
manual_job_stopped_instance = ManualJobStopped.from_json(json)
# print the JSON string representation of the object
print(ManualJobStopped.to_json())

# convert the object into a dict
manual_job_stopped_dict = manual_job_stopped_instance.to_dict()
# create an instance of ManualJobStopped from a dict
manual_job_stopped_from_dict = ManualJobStopped.from_dict(manual_job_stopped_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
