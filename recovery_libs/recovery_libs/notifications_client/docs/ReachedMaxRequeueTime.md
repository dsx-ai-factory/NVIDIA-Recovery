# ReachedMaxRequeueTime

The job has reached the maximum requeue time.  Examples:     >>> message = ReachedMaxRequeueTime(     ...     job_id=\"123\", job_attempt_index=7, job_name=\"some-job\", message=\"hello message\"     ... )     >>> expected_message = (     ...     \"The job 123(7) job name some-job has reached the max requeue time, reason: hello message.\"     ... )     >>> assert_equal(message.render_message(), expected_message)      >>> assert_equal(message.render_title(), \"Max Requeue Reached, JobId: 123(7)\")

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ReachedMaxRequeueTime']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**message** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.reached_max_requeue_time import ReachedMaxRequeueTime

# TODO update the JSON string below
json = "{}"
# create an instance of ReachedMaxRequeueTime from a JSON string
reached_max_requeue_time_instance = ReachedMaxRequeueTime.from_json(json)
# print the JSON string representation of the object
print(ReachedMaxRequeueTime.to_json())

# convert the object into a dict
reached_max_requeue_time_dict = reached_max_requeue_time_instance.to_dict()
# create an instance of ReachedMaxRequeueTime from a dict
reached_max_requeue_time_from_dict = ReachedMaxRequeueTime.from_dict(reached_max_requeue_time_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
