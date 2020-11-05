# Wormhole

Super minimal framework for RPC and messaging over redis in python

## Why?
Fun and learning, also it is much easier to setup comparing to other cloud-messaging solutions and it is fast!

## Quick start


*Receiver*
```
from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()


# We can call this from any wormhole in any python process
def remote_sum(items: list):
    return sum(items)


wormhole.register_handler("sum", remote_sum)
wormhole.process_blocking()

```

*Sender*
```
from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()

# Send blocking via .send().wait()
assert wormhole.send("sum", [1, 1, 3]).wait() == sum([1, 1, 3])

```


## Communicating with a persistent Receiver
When sending to a wormhole queue, we don't know which receiver will handle the request
we can use the returned session object to keep communicating with the replied service

*Sender*
```
from wormhole.setup import basic_wormhole_setup

wormhole = basic_wormhole_setup()

session: "WormholeSession" = wormhole.send("sum", [1, 1, 3])
result_sum = session.wait()

# We can keep using the session object to communicate with the same receiver
# This will ensure 
print("Sum: " + str(wormhole.send("sum", [5,5,5], session=session).wait())

```
