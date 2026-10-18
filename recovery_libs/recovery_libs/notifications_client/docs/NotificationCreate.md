# NotificationCreate

Payload for creating a schema.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**recipient** | **str** | The recipient of the notification. |
**job_id** | **str** |  | [optional]
**title** | **str** |  | [optional]
**message** | **str** |  | [optional]
**template_data** | [**TemplateData**](TemplateData.md) |  | [optional]
**type** | [**NotificationType**](NotificationType.md) |  | [optional]

## Example

```python
from recovery_libs.notifications_client.models.notification_create import NotificationCreate

# TODO update the JSON string below
json = "{}"
# create an instance of NotificationCreate from a JSON string
notification_create_instance = NotificationCreate.from_json(json)
# print the JSON string representation of the object
print(NotificationCreate.to_json())

# convert the object into a dict
notification_create_dict = notification_create_instance.to_dict()
# create an instance of NotificationCreate from a dict
notification_create_from_dict = NotificationCreate.from_dict(notification_create_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
