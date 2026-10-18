# JobRepr

A representation of a job, including id and attempt index.  Examples:     >>> JobRepr(job_id=\"456\", job_attempt_index=7).render_message()     '456(7)'

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/partials/JobRepr']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |

## Example

```python
from recovery_libs.notifications_client.models.job_repr import JobRepr

# TODO update the JSON string below
json = "{}"
# create an instance of JobRepr from a JSON string
job_repr_instance = JobRepr.from_json(json)
# print the JSON string representation of the object
print(JobRepr.to_json())

# convert the object into a dict
job_repr_dict = job_repr_instance.to_dict()
# create an instance of JobRepr from a dict
job_repr_from_dict = JobRepr.from_dict(job_repr_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
