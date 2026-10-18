# ErrorAttribution

A representation of an error attribution that's made of a JobExecutionError and a list of NodeEvent and GlobalEvent.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/partials/ErrorAttribution']
**job_execution_error** | [**JobExecutionError**](JobExecutionError.md) |  | 
**node_events** | [**List[NodeEvent]**](NodeEvent.md) |  | 
**global_events** | [**List[GlobalEvent]**](GlobalEvent.md) |  | [optional] 

## Example

```python
from recovery_libs.notifications_client.models.error_attribution import ErrorAttribution

# TODO update the JSON string below
json = "{}"
# create an instance of ErrorAttribution from a JSON string
error_attribution_instance = ErrorAttribution.from_json(json)
# print the JSON string representation of the object
print(ErrorAttribution.to_json())

# convert the object into a dict
error_attribution_dict = error_attribution_instance.to_dict()
# create an instance of ErrorAttribution from a dict
error_attribution_from_dict = ErrorAttribution.from_dict(error_attribution_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


