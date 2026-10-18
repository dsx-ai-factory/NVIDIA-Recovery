# JobName

A representation of the job name.  The default template just shows the job name, but it can be overridden to show more interesting representations.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/partials/JobName']
**job_name** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.job_name import JobName

# TODO update the JSON string below
json = "{}"
# create an instance of JobName from a JSON string
job_name_instance = JobName.from_json(json)
# print the JSON string representation of the object
print(JobName.to_json())

# convert the object into a dict
job_name_dict = job_name_instance.to_dict()
# create an instance of JobName from a dict
job_name_from_dict = JobName.from_dict(job_name_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
