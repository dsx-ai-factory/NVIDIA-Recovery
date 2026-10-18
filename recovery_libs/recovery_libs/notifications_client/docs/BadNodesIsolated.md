# BadNodesIsolated

Advice to exclude nodes from a job attempt has been issued.  Examples:     >>> message = BadNodesIsolated(     ...     job_id=\"456\",     ...     job_attempt_index=7,     ...     job_name=\"some-job\",     ...     bad_nodes={\"aaa\": [\"a1\", \"a2\"], \"bbb\": [\"b1\"]},     ...     non_isolated_nodes={\"ccc\": [\"c1\"], \"ddd\": [\"d1\", \"d2\"]},     ...     user=\"some-user\",     ... )     >>> expected_message = '_'_'\\     ... Advice to exclude nodes {'aaa': ['a1', 'a2'], 'bbb': ['b1']} from job 456(7) (job name: some-job) issued.     ... '_'_'     >>> assert_equal(message.render_message(), expected_message)      >>> expected_title = \"Advice to isolate nodes for job 456 issued\"     >>> assert_equal(message.render_title(), expected_title)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**template_id** | **str** |  | [optional] [default to '/templates/BadNodesIsolated']
**job_id** | **str** |  |
**job_attempt_index** | **int** |  |
**job_name** | **str** |  |
**bad_nodes** | **Dict[str, List[str]]** |  |
**non_isolated_nodes** | **Dict[str, List[str]]** |  |
**user** | **str** |  |

## Example

```python
from recovery_libs.notifications_client.models.bad_nodes_isolated import BadNodesIsolated

# TODO update the JSON string below
json = "{}"
# create an instance of BadNodesIsolated from a JSON string
bad_nodes_isolated_instance = BadNodesIsolated.from_json(json)
# print the JSON string representation of the object
print(BadNodesIsolated.to_json())

# convert the object into a dict
bad_nodes_isolated_dict = bad_nodes_isolated_instance.to_dict()
# create an instance of BadNodesIsolated from a dict
bad_nodes_isolated_from_dict = BadNodesIsolated.from_dict(bad_nodes_isolated_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
