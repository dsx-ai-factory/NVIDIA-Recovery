# NoMoreReleasingJobs

autonomous recovery engine can't release any more jobs.  Examples:     >>> message = NoMoreReleasingJobs(job_name=\"some-job\", user=\"some-user\")     >>> expected_message = '_'_'\\     ... No more jobs in `PENDING(JobHeldUser)` state for job name: some-job - won't release any new jobs.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"No more pending jobs for some-job\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/NoMoreReleasingJobs']
**job_name** | **str** |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.no_more_releasing_jobs import NoMoreReleasingJobs

# TODO update the JSON string below
json = "{}"
# create an instance of NoMoreReleasingJobs from a JSON string
no_more_releasing_jobs_instance = NoMoreReleasingJobs.from_json(json)
# print the JSON string representation of the object
print(NoMoreReleasingJobs.to_json())

# convert the object into a dict
no_more_releasing_jobs_dict = no_more_releasing_jobs_instance.to_dict()
# create an instance of NoMoreReleasingJobs from a dict
no_more_releasing_jobs_from_dict = NoMoreReleasingJobs.from_dict(no_more_releasing_jobs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
