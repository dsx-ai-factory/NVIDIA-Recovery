# SuccessfullyFinished

The job successfully finished.  Examples:     >>> message = SuccessfullyFinished(job_id=\"123\", job_attempt_index=7, job_name=\"some-job\")     >>> expected_message = \"Job 123(7) (some-job) finished.\"     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Finished job 123\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/SuccessfullyFinished']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.successfully_finished import SuccessfullyFinished

# TODO update the JSON string below
json = "{}"
# create an instance of SuccessfullyFinished from a JSON string
successfully_finished_instance = SuccessfullyFinished.from_json(json)
# print the JSON string representation of the object
print(SuccessfullyFinished.to_json())

# convert the object into a dict
successfully_finished_dict = successfully_finished_instance.to_dict()
# create an instance of SuccessfullyFinished from a dict
successfully_finished_from_dict = SuccessfullyFinished.from_dict(successfully_finished_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
