Durable Call: Durable Local and Remote Function Calls
=====================================================

Durable call is a library for implementing durable functions
from fragile functions.

A function is said to be fragile
if any of the following conditions are true:
a) the function may fail intermittently,
b) the function call has side effects,
and thus for a given input it should only be run once.

A durable function is a fault tolerant and idempotent version
of a fragile function.
This fault tolerance and idempotency is implemented by the runtime.
When a durable function is called,
the runtime system first saves the parameters of the function call
onto a file on disk.
Next it repeatedly tries to call the function
until it is able to execute it.
Once the function has been executed,
the result is also saved
onto a file on disk.
If the function is called in the future with the same arguments,
the system will just return the saved result
from the previous invocation.

Durable function call persists across process invocations.
If a durable function call is initiated,
and the process dies before it has been completed,
the runtime with continue to execute it
if the process is restarted.
