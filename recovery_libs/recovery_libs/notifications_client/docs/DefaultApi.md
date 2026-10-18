# recovery_libs.notifications_client.DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**metrics**](DefaultApi.md#metrics) | **GET** /metrics | Metrics


# **metrics**
> object metrics()

Metrics

Endpoint that serves Prometheus metrics.

### Example


```python
import recovery_libs.notifications_client
from recovery_libs.notifications_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = recovery_libs.notifications_client.Configuration(host="http://localhost")


# Enter a context with an instance of the API client
with recovery_libs.notifications_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = recovery_libs.notifications_client.DefaultApi(api_client)

    try:
        # Metrics
        api_response = api_instance.metrics()
        print("The response of DefaultApi->metrics:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->metrics: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)
