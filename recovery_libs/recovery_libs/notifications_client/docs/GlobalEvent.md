# GlobalEvent

A representation of a global attribution event (job-level, not tied to specific nodes).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/partials/GlobalEvent']
**error_attribution** | **str** |  | 
**error_attribution_sources** | **List[str]** |  | 

## Example

```python
from recovery_libs.notifications_client.models.global_event import GlobalEvent

# TODO update the JSON string below
json = "{}"
# create an instance of GlobalEvent from a JSON string
global_event_instance = GlobalEvent.from_json(json)
# print the JSON string representation of the object
print(GlobalEvent.to_json())

# convert the object into a dict
global_event_dict = global_event_instance.to_dict()
# create an instance of GlobalEvent from a dict
global_event_from_dict = GlobalEvent.from_dict(global_event_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


