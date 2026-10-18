# ItemsNotificationPublic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**items** | [**List[NotificationPublic]**](NotificationPublic.md) |  |
**total_items** | **int** |  |

## Example

```python
from recovery_libs.notifications_client.models.items_notification_public import ItemsNotificationPublic

# TODO update the JSON string below
json = "{}"
# create an instance of ItemsNotificationPublic from a JSON string
items_notification_public_instance = ItemsNotificationPublic.from_json(json)
# print the JSON string representation of the object
print(ItemsNotificationPublic.to_json())

# convert the object into a dict
items_notification_public_dict = items_notification_public_instance.to_dict()
# create an instance of ItemsNotificationPublic from a dict
items_notification_public_from_dict = ItemsNotificationPublic.from_dict(items_notification_public_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
