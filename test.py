import asyncio
from functools import wraps


def logger(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            res = await func(*args, **kwargs)
            return res
        except Exception as e:
            print("Ошибка", e)

    return wrapper


async def task1():
    print("task 1 started")
    await asyncio.sleep(2)
    print("task 1 finished")
    return 1


async def task2():
    print("task 2 started")
    await asyncio.sleep(1)
    print(1 / 0)
    print("task 2 finished")
    return 2


async def task3():
    print("task 3 started")
    await asyncio.sleep(1)
    print("task 3 finished")
    return 3


async def main():
    results = await asyncio.gather(task1(), task2(), task3(), return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            print("Ошибка: ", result)
        else:
            print("Результат: ", result)

    results = []
    try:
        async with asyncio.TaskGroup() as group:
            results.append(group.create_task(task1()))
            results.append(group.create_task(task2()))
            results.append(group.create_task(task3()))
    except* Exception as e:
        print("exception:", e.exceptions)

    for result in results:
        if not result.cancelled():
            print(result.result() if not result.exception() else result.exception())


if __name__ == "__main__":
    asyncio.run(main())
