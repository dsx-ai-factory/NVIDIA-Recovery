# NotificationCreateTemplateData


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/ManualJobAction']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**user** | **str** |  |
**message** | **str** |  |
**action** | **str** |  |
**event_type** | **str** |  |
**event_body** | **str** |  |
**bad_nodes** | **Dict[str, List[str]]** |  |
**non_isolated_nodes** | **Dict[str, List[str]]** |  |
**attribution_analyzer_output** | **str** |  |
**bad_job** | [**JobRepr**](JobRepr.md) |  |
**triggerer** | [**User**](User.md) |  |

## Example

```python
from recovery_libs.notifications_client.models.notification_create_template_data import NotificationCreateTemplateData

# TODO update the JSON string below
json = "{}"
# create an instance of NotificationCreateTemplateData from a JSON string
notification_create_template_data_instance = NotificationCreateTemplateData.from_json(json)
# print the JSON string representation of the object
print(NotificationCreateTemplateData.to_json())

# convert the object into a dict
notification_create_template_data_dict = notification_create_template_data_instance.to_dict()
# create an instance of NotificationCreateTemplateData from a dict
notification_create_template_data_from_dict = NotificationCreateTemplateData.from_dict(
    notification_create_template_data_dict
)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
