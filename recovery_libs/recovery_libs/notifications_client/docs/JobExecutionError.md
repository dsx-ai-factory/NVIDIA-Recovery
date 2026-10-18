# JobExecutionError

A representation of a job execution error.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/partials/JobExecutionError']
**error_message** | **str** |  | [optional]
**error_signature** | **str** |  | [optional]

## Example

```python
from recovery_libs.notifications_client.models.job_execution_error import JobExecutionError

# TODO update the JSON string below
json = "{}"
# create an instance of JobExecutionError from a JSON string
job_execution_error_instance = JobExecutionError.from_json(json)
# print the JSON string representation of the object
print(JobExecutionError.to_json())

# convert the object into a dict
job_execution_error_dict = job_execution_error_instance.to_dict()
# create an instance of JobExecutionError from a dict
job_execution_error_from_dict = JobExecutionError.from_dict(job_execution_error_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
