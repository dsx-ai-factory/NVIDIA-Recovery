# ManualJobStarted

A user has manually requeued a job via cockpit.  Examples:     >>> message = ManualJobStarted(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     triggerer=User(user=\"job-killer-guy\"),     ...     user=\"some-person\",     ... )     >>> expected_message = '_'_'\\     ... Job 456(7) (job name: some-job) has been `STARTED` manually via Cockpit by job-killer-guy.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Manually started job 456\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobStarted']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**triggerer** | [**User**](User.md) |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.manual_job_started import ManualJobStarted

# TODO update the JSON string below
json = "{}"
# create an instance of ManualJobStarted from a JSON string
manual_job_started_instance = ManualJobStarted.from_json(json)
# print the JSON string representation of the object
print(ManualJobStarted.to_json())

# convert the object into a dict
manual_job_started_dict = manual_job_started_instance.to_dict()
# create an instance of ManualJobStarted from a dict
manual_job_started_from_dict = ManualJobStarted.from_dict(manual_job_started_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
