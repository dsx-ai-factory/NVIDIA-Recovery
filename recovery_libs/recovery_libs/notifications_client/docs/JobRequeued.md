# JobRequeued

Advice to requeue a job attempt has been issued.  Examples:     >>> message = JobRequeued(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     event_body=\"this is the event body\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Advice to requeue job 456(7) (job name: some-job) issued.     ... Reason: `SILLY`.     ... Log: `this is the event body`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Advice to requeue job 456 issued\"     >>> assert_equal(message.render_title(), expected_title)      The `event_body` is optional:      >>> message = JobRequeued(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     event_type=\"SILLY\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Advice to requeue job 456(7) (job name: some-job) issued.     ... Reason: `SILLY`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/JobRequeued']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  | [optional]
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.job_requeued import JobRequeued

# TODO update the JSON string below
json = "{}"
# create an instance of JobRequeued from a JSON string
job_requeued_instance = JobRequeued.from_json(json)
# print the JSON string representation of the object
print(JobRequeued.to_json())

# convert the object into a dict
job_requeued_dict = job_requeued_instance.to_dict()
# create an instance of JobRequeued from a dict
job_requeued_from_dict = JobRequeued.from_dict(job_requeued_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
