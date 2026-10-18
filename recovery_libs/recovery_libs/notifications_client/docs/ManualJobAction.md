# ManualJobAction

A user has performed a manual action via cockpit.  DEPRECATED: Use ManualJobPaused, ManualJobStopped, ManualJobRequeued, or ManualJobStarted instead.  Examples:     >>> message = ManualJobAction(     ...     action=\"KILLED\",     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     triggerer=User(user=\"job-killer-guy\"),     ...     user=\"some-person\",     ... )     >>> expected_message = '_'_'\\     ... autonomous recovery engine has manually `KILLED` the job 456(7) (job name: some-job) via Cockpit by job-killer-guy.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Manually KILLED job 456\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobAction']
**action** | **str** |  |
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**triggerer** | [**User**](User.md) |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.manual_job_action import ManualJobAction

# TODO update the JSON string below
json = "{}"
# create an instance of ManualJobAction from a JSON string
manual_job_action_instance = ManualJobAction.from_json(json)
# print the JSON string representation of the object
print(ManualJobAction.to_json())

# convert the object into a dict
manual_job_action_dict = manual_job_action_instance.to_dict()
# create an instance of ManualJobAction from a dict
manual_job_action_from_dict = ManualJobAction.from_dict(manual_job_action_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
