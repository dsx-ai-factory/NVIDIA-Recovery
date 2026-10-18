# UserPreferencesPut

Payload for creating/updating user preferences.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email_enabled** | **bool** |  | [optional] [default to True]
**slack_enabled** | **bool** |  | [optional] [default to True]
**notification_types** | [**NotificationTypePreferences**](NotificationTypePreferences.md) |  | [optional]

## Example

```python
from recovery_libs.notifications_client.models.user_preferences_put import UserPreferencesPut

# TODO update the JSON string below
json = "{}"
# create an instance of UserPreferencesPut from a JSON string
user_preferences_put_instance = UserPreferencesPut.from_json(json)
# print the JSON string representation of the object
print(UserPreferencesPut.to_json())

# convert the object into a dict
user_preferences_put_dict = user_preferences_put_instance.to_dict()
# create an instance of UserPreferencesPut from a dict
user_preferences_put_from_dict = UserPreferencesPut.from_dict(user_preferences_put_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
