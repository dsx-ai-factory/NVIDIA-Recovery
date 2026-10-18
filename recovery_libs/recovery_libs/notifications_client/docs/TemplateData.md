# TemplateData

A template_id and the data associated with it.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobStarted']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**user** | **str** |  |
**error_message** | **str** |  | [optional]
**error_signature** | **str** |  | [optional]
**node** | **str** |  |
**exclusion_type** | **str** |  |
**error_attribution** | **str** |  |
**error_attribution_sources** | **List[str]** |  |
**job_execution_error** | [**JobExecutionError**](JobExecutionError.md) |  |
**node_events** | [**List[NodeEvent]**](NodeEvent.md) |  |
**global_events** | [**List[GlobalEvent]**](GlobalEvent.md) |  | [optional]
**message** | **str** |  |
**action** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  | [optional]
**reason** | **str** |  |
**bad_nodes** | **Dict[str, List[str]]** |  |
**non_isolated_nodes** | **Dict[str, List[str]]** |  |
**anomaly_type** | **str** |  |
**error_attributions** | [**List[ErrorAttribution]**](ErrorAttribution.md) |  |
**summary** | **str** |  | [optional]
**insight** | **str** |  | [optional]
**bad_job** | [**JobRepr**](JobRepr.md) |  |
**triggerer** | [**User**](User.md) |  |

## Example

```python
from recovery_libs.notifications_client.models.template_data import TemplateData

# TODO update the JSON string below
json = "{}"
# create an instance of TemplateData from a JSON string
template_data_instance = TemplateData.from_json(json)
# print the JSON string representation of the object
print(TemplateData.to_json())

# convert the object into a dict
template_data_dict = template_data_instance.to_dict()
# create an instance of TemplateData from a dict
template_data_from_dict = TemplateData.from_dict(template_data_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
