# ReleasedJob

Advice to release a job attempt has been issued.  Examples:     >>> message = ReleasedJob(     ...     job_id=\"123\",     ...     job_attempt_index=4,     ...     job_name=\"some-job\",     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Advice to release job 123(4) (job name: some-job) issued.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Advice to release job 123 issued\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ReleasedJob']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.released_job import ReleasedJob

# TODO update the JSON string below
json = "{}"
# create an instance of ReleasedJob from a JSON string
released_job_instance = ReleasedJob.from_json(json)
# print the JSON string representation of the object
print(ReleasedJob.to_json())

# convert the object into a dict
released_job_dict = released_job_instance.to_dict()
# create an instance of ReleasedJob from a dict
released_job_from_dict = ReleasedJob.from_dict(released_job_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
