# BasicJobAction

A generic notification that autonomous recovery engine has taken some action on some job.  DEPRECATED: Use JobStopped and JobRequeued instead.  Examples:     >>> message = BasicJobAction(     ...     action=\"KILLED\",     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     event_body=\"this is the event body\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... autonomous recovery engine has `KILLED` the job 456(7) (job name: some-job) for being `SILLY`.     ... Log: `this is the event body`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"KILLED job 456\"     >>> assert_equal(message.render_title(), expected_title)      The `event_body` is optional:      >>> message = BasicJobAction(     ...     action=\"KILLED\",     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... autonomous recovery engine has `KILLED` the job 456(7) (job name: some-job) for being `SILLY`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/BasicJobAction']
**action** | **str** |  |
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  | [optional]
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.basic_job_action import BasicJobAction

# TODO update the JSON string below
json = "{}"
# create an instance of BasicJobAction from a JSON string
basic_job_action_instance = BasicJobAction.from_json(json)
# print the JSON string representation of the object
print(BasicJobAction.to_json())

# convert the object into a dict
basic_job_action_dict = basic_job_action_instance.to_dict()
# create an instance of BasicJobAction from a dict
basic_job_action_from_dict = BasicJobAction.from_dict(basic_job_action_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
