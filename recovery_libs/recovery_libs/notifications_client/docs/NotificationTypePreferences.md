# NotificationTypePreferences


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**optional** | **bool** |  | [optional] [default to False]
**info** | **bool** |  | [optional] [default to True]
**performance_alert** | **bool** |  | [optional] [default to True]
**warning** | **bool** |  | [optional] [default to True]

## Example

```python
from recovery_libs.notifications_client.models.notification_type_preferences import NotificationTypePreferences

# TODO update the JSON string below
json = "{}"
# create an instance of NotificationTypePreferences from a JSON string
notification_type_preferences_instance = NotificationTypePreferences.from_json(json)
# print the JSON string representation of the object
print(NotificationTypePreferences.to_json())

# convert the object into a dict
notification_type_preferences_dict = notification_type_preferences_instance.to_dict()
# create an instance of NotificationTypePreferences from a dict
notification_type_preferences_from_dict = NotificationTypePreferences.from_dict(notification_type_preferences_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
