# recovery_libs.notifications_client.NotificationsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**notification_create**](NotificationsApi.md#notification_create) | **POST** /notification | Create a new notification
[**notifications_get**](NotificationsApi.md#notifications_get) | **GET** /notifications | Get notifications


# **notification_create**
> NotificationPublic notification_create(notification_create)

Create a new notification

### Example


```python
import recovery_libs.notifications_client
from recovery_libs.notifications_client.models.notification_create import NotificationCreate
from recovery_libs.notifications_client.models.notification_public import NotificationPublic
from recovery_libs.notifications_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = recovery_libs.notifications_client.Configuration(host="http://localhost")


# Enter a context with an instance of the API client
with recovery_libs.notifications_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = recovery_libs.notifications_client.NotificationsApi(api_client)
    notification_create = recovery_libs.notifications_client.NotificationCreate()  # NotificationCreate |

    try:
        # Create a new notification
        api_response = api_instance.notification_create(notification_create)
        print("The response of NotificationsApi->notification_create:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling NotificationsApi->notification_create: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **notification_create** | [**NotificationCreate**](NotificationCreate.md)|  |

### Return type

[**NotificationPublic**](NotificationPublic.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **notifications_get**
> ItemsNotificationPublic notifications_get(id=id, recipient=recipient, job_id=job_id, limit=limit, offset=offset, created_after=created_after, created_before=created_before)

Get notifications

### Example


```python
import recovery_libs.notifications_client
from recovery_libs.notifications_client.models.items_notification_public import ItemsNotificationPublic
from recovery_libs.notifications_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = recovery_libs.notifications_client.Configuration(host="http://localhost")


# Enter a context with an instance of the API client
with recovery_libs.notifications_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = recovery_libs.notifications_client.NotificationsApi(api_client)
    id = "id_example"  # str | The id of the notification. (optional)
    recipient = "recipient_example"  # str | The recipient the notifications were sent to. (optional)
    job_id = "job_id_example"  # str | The job_id associated with the job. (optional)
    limit = 50  # int | The maximum number of records to return. (optional) (default to 50)
    offset = 0  # int | The offset to start the records from. (optional) (default to 0)
    created_after = (
        "2013-10-20T19:20:30+01:00"  # datetime | Only show notifications created after this time. (optional)
    )
    created_before = (
        "2013-10-20T19:20:30+01:00"  # datetime | Only show notifications created before this time. (optional)
    )

    try:
        # Get notifications
        api_response = api_instance.notifications_get(
            id=id,
            recipient=recipient,
            job_id=job_id,
            limit=limit,
            offset=offset,
            created_after=created_after,
            created_before=created_before,
        )
        print("The response of NotificationsApi->notifications_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling NotificationsApi->notifications_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **str**| The id of the notification. | [optional]
 **recipient** | **str**| The recipient the notifications were sent to. | [optional]
 **job_id** | **str**| The job_id associated with the job. | [optional]
 **limit** | **int**| The maximum number of records to return. | [optional] [default to 50]
 **offset** | **int**| The offset to start the records from. | [optional] [default to 0]
 **created_after** | **datetime**| Only show notifications created after this time. | [optional]
 **created_before** | **datetime**| Only show notifications created before this time. | [optional]

### Return type

[**ItemsNotificationPublic**](ItemsNotificationPublic.md)

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
