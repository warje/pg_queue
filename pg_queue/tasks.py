import json


async def print_pretty(tx, params):
    print(json.dumps(params, indent=True))


ALL_TASKS = {
    'print_pretty': print_pretty
}
