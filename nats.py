import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError

async def main():
    # connect to nats server
    nc = await nats.connect("nats://demo.nats.io:4222")

    # callback function
    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print(f"Received a message on '{subject} {reply}': {data}")

    # subscribe to channel1
    sub1 = await nc.subscribe(subject="channel1",cb=message_handler)
    sub2 = await nc.subscribe(subject="channel2", cb=message_handler)

    # publish messages on channel1
    await nc.publish("channel1", b'Hello from channel1')
    await nc.publish("channel1", b'Sensors are working')
    await nc.publish("channel1", b'Bye from channel1')

    # publish messages on channel2
    await nc.publish("channel2", b'Hello from channel2')
    await nc.publish("channel2", b'Rocket is healthy')
    await nc.publish("channel2", b'Bye from channel2')

    #await sub.unsubscribe()

    # will act as a cb function
    # msg must be a message sent by nc.request(), NOT nc.publish() bc nc.request means that subscribers of the
    # channel can also reply. They reply by sending a message on msg.reply, which is the reply address for that
    # channel. In this case, a request is being sent on the "help" channel. The variable "sub" is subscribed to
    # that channel with the "help_request" function as its callback function, giving it the ability to reply to
    # messages on that channel (as long as messages are being set via nc.request()). It replies by sending a
    # message not to "help", but to msg.reply, which is the unique reply address to that prtclr message that was
    # just sent on "help".
    async def help_request(msg):
        print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
        await nc.publish(msg.reply, b'I help you')

    # subscribe to "help" channel
    # If a message is sent to "help", help_request is called, where the parameter "msg" is
    # the message that was sent to "help".
    sub = await nc.subscribe(subject="help", cb=help_request) # can also include queue="workers"

    # Send a message on the channel "help", and wait for a response until the timeout occurs.
    # "help" is a channel that's unique to the requestor
    try:
        response = await nc.request("help", b'Plz help me', timeout=3)
        print(f"Received response: {response.data.decode()}")
    except TimeoutError:
        print("Request timed out")

    # Unsubscribe before ending connection
    await sub.unsubscribe()

    # End connection with NATS, better than close()
    await nc.drain()

if __name__ == '__main__':
    asyncio.run(main())
