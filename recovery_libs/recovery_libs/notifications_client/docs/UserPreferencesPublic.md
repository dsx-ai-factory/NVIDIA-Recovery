# UserPreferencesPublic

A user's notification preferences.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email_enabled** | **bool** |  |
**slack_enabled** | **bool** |  |
**notification_types** | [**NotificationTypePreferences**](NotificationTypePreferences.md) |  |

## Example

```python
from recovery_libs.notifications_client.models.user_preferences_public import UserPreferencesPublic

# TODO update the JSON string below
json = "{}"
# create an instance of UserPreferencesPublic from a JSON string
user_preferences_public_instance = UserPreferencesPublic.from_json(json)
# print the JSON string representation of the object
print(UserPreferencesPublic.to_json())

# convert the object into a dict
user_preferences_public_dict = user_preferences_public_instance.to_dict()
# create an instance of UserPreferencesPublic from a dict
user_preferences_public_from_dict = UserPreferencesPublic.from_dict(user_preferences_public_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
