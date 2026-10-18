# recovery_libs.notifications_client.PreferencesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**user_preferences_get**](PreferencesApi.md#user_preferences_get) | **GET** /user-preferences/{username} | Retrieve a user&#39;s notification preferences.
[**user_preferences_put**](PreferencesApi.md#user_preferences_put) | **PUT** /user-preferences/{username} | Create or update a user&#39;s notification preferences.


# **user_preferences_get**
> UserPreferencesPublic user_preferences_get(username)

Retrieve a user's notification preferences.

### Example


```python
import recovery_libs.notifications_client
from recovery_libs.notifications_client.models.user_preferences_public import UserPreferencesPublic
from recovery_libs.notifications_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = recovery_libs.notifications_client.Configuration(host="http://localhost")


# Enter a context with an instance of the API client
with recovery_libs.notifications_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = recovery_libs.notifications_client.PreferencesApi(api_client)
    username = "username_example"  # str |

    try:
        # Retrieve a user's notification preferences.
        api_response = api_instance.user_preferences_get(username)
        print("The response of PreferencesApi->user_preferences_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PreferencesApi->user_preferences_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**|  |

### Return type

[**UserPreferencesPublic**](UserPreferencesPublic.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **user_preferences_put**
> UserPreferencesPublic user_preferences_put(username, user_preferences_put)

Create or update a user's notification preferences.

### Example


```python
import recovery_libs.notifications_client
from recovery_libs.notifications_client.models.user_preferences_public import UserPreferencesPublic
from recovery_libs.notifications_client.models.user_preferences_put import UserPreferencesPut
from recovery_libs.notifications_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = recovery_libs.notifications_client.Configuration(host="http://localhost")


# Enter a context with an instance of the API client
with recovery_libs.notifications_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = recovery_libs.notifications_client.PreferencesApi(api_client)
    username = "username_example"  # str |
    user_preferences_put = recovery_libs.notifications_client.UserPreferencesPut()  # UserPreferencesPut |

    try:
        # Create or update a user's notification preferences.
        api_response = api_instance.user_preferences_put(username, user_preferences_put)
        print("The response of PreferencesApi->user_preferences_put:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PreferencesApi->user_preferences_put: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**|  |
 **user_preferences_put** | [**UserPreferencesPut**](UserPreferencesPut.md)|  |

### Return type

[**UserPreferencesPublic**](UserPreferencesPublic.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**201** | Successfully created. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)
