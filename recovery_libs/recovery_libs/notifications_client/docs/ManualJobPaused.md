# ManualJobPaused

A user has manually paused a job via cockpit.  Examples:     >>> message = ManualJobPaused(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     triggerer=User(user=\"job-killer-guy\"),     ...     user=\"some-person\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(7) (job name: some-job) has been `PAUSED` manually via Cockpit by job-killer-guy.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Manually paused job 456\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobPaused']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**triggerer** | [**User**](User.md) |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.manual_job_paused import ManualJobPaused

# TODO update the JSON string below
json = "{}"
# create an instance of ManualJobPaused from a JSON string
manual_job_paused_instance = ManualJobPaused.from_json(json)
# print the JSON string representation of the object
print(ManualJobPaused.to_json())

# convert the object into a dict
manual_job_paused_dict = manual_job_paused_instance.to_dict()
# create an instance of ManualJobPaused from a dict
manual_job_paused_from_dict = ManualJobPaused.from_dict(manual_job_paused_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
