# NotificationPublic

A fully formed notification object.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**recipient** | **str** | The recipient of the notification. |
**job_id** | **str** |  | [optional]
**id** | **str** | The unique id of the notification. | [optional]
**title** | **str** | The title of the notification. |
**message** | **str** | The notification&#39;s message body. |
**template_data** | [**TemplateData**](TemplateData.md) |  | [optional]
**type** | [**NotificationType**](NotificationType.md) | The type of notification. |
**created_time** | **datetime** | When was the notification created. | [optional]
**via_slack** | [**NotificationStatus**](NotificationStatus.md) | The current status of this notification being sent via slack. | [optional]
**via_email** | [**NotificationStatus**](NotificationStatus.md) | The current status of this notification being sent via email. | [optional]
**via_callback** | [**NotificationStatus**](NotificationStatus.md) | The current status of this notification being sent via http request callback. | [optional]
**status** | [**NotificationStatus**](NotificationStatus.md) | DEPRECATED - The current status of the notification.  This status always applies to the slack notification. Use &#x60;via_slack&#x60; instead. | [optional]
**channel** | [**NotificationChannel**](NotificationChannel.md) | DEPRECATED - The channel the notification was sent to. | [optional]

## Example

```python
from recovery_libs.notifications_client.models.notification_public import NotificationPublic

# TODO update the JSON string below
json = "{}"
# create an instance of NotificationPublic from a JSON string
notification_public_instance = NotificationPublic.from_json(json)
# print the JSON string representation of the object
print(NotificationPublic.to_json())

# convert the object into a dict
notification_public_dict = notification_public_instance.to_dict()
# create an instance of NotificationPublic from a dict
notification_public_from_dict = NotificationPublic.from_dict(notification_public_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
