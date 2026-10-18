# StraggleJob

Show that autonomous recovery engine has found a straggler.  DEPRECATED: Use GpuThroughputDropped instead.  Examples:     >>> message = StraggleJob(     ...     job_id=\"456\",     ...     event_type=\"SILLY\",     ...     event_body=\"this is the event body\",     ... )     >>> expected_message = '_'_'\\     ... autonomous recovery engine found job 456 for being `SILLY`.     ... Log: `this is the event body`     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"STRAGGLE job 456\"     >>> assert_equal(message.render_title(), expected_title)      The `event_body` is optional:      >>> message = StraggleJob(     ...     job_id=\"456\",     ...     event_type=\"SILLY\",     ... )     >>> expected_message = '_'_'\\     ... autonomous recovery engine found job 456 for being `SILLY`.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/StraggleJob']
**job_id** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  | [optional]

## Example

```python
from recovery_libs.notifications_client.models.straggle_job import StraggleJob

# TODO update the JSON string below
json = "{}"
# create an instance of StraggleJob from a JSON string
straggle_job_instance = StraggleJob.from_json(json)
# print the JSON string representation of the object
print(StraggleJob.to_json())

# convert the object into a dict
straggle_job_dict = straggle_job_instance.to_dict()
# create an instance of StraggleJob from a dict
straggle_job_from_dict = StraggleJob.from_dict(straggle_job_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
