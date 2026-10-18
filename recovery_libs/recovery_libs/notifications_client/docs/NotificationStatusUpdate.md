# NotificationStatusUpdate

Payload for updating a notification's status.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | [**NotificationStatus**](NotificationStatus.md) | The new status to set. |

## Example

```python
from recovery_libs.notifications_client.models.notification_status_update import NotificationStatusUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of NotificationStatusUpdate from a JSON string
notification_status_update_instance = NotificationStatusUpdate.from_json(json)
# print the JSON string representation of the object
print(NotificationStatusUpdate.to_json())

# convert the object into a dict
notification_status_update_dict = notification_status_update_instance.to_dict()
# create an instance of NotificationStatusUpdate from a dict
notification_status_update_from_dict = NotificationStatusUpdate.from_dict(notification_status_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
