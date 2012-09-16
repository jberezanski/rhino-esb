using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Messaging;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Rhino.ServiceBus.Config;
using Rhino.ServiceBus.Exceptions;
using Rhino.ServiceBus.Impl;
using Rhino.ServiceBus.Internal;
using Rhino.ServiceBus.Msmq;
using Rhino.ServiceBus.RhinoQueues;
using Xunit;

namespace Rhino.ServiceBus.Tests.Bugs
{
    public class When_instance_consumer_handles_two_messages : MsmqTestBase
    {
        private WindsorContainer publisherContainer;

        private WindsorContainer subscriberContainer;

        private const string PhtSubscriptionsPath = "test.esent";

        public When_instance_consumer_handles_two_messages()
        {
            if (Directory.Exists(PhtSubscriptionsPath))
                Directory.Delete(PhtSubscriptionsPath, true);
        }

        private IStartableServiceBus CreatePublisher()
        {
            var busConfig =new BusConfigurationSection();
            busConfig.Bus.Endpoint = this.TestQueueUri.Uri.ToString();
            busConfig.Bus.ThreadCount = 1;
            busConfig.Bus.NumberOfRetries = 1;

            this.publisherContainer = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseConfiguration(busConfig)
                .UseCastleWindsor(publisherContainer)
                .Configure();
            return publisherContainer.Resolve<IStartableServiceBus>();
        }

        private IStartableServiceBus CreatePublisherWithPHT()
        {
            var busConfig = new BusConfigurationSection();
            busConfig.Bus.Endpoint = this.TestQueueUri.Uri.ToString();
            busConfig.Bus.ThreadCount = 1;
            busConfig.Bus.NumberOfRetries = 1;

            this.publisherContainer = new WindsorContainer();

            this.publisherContainer.Register(
                Component
                .For<ISubscriptionStorage>()
                .ImplementedBy<PhtSubscriptionStorage>()
                .DependsOn(
                    new
                    {
                        subscriptionPath = PhtSubscriptionsPath
                    })
                .LifestyleSingleton()
                .IsDefault());

            new RhinoServiceBusConfiguration()
                .UseConfiguration(busConfig)
                .UseCastleWindsor(publisherContainer)
                .Configure();
            return publisherContainer.Resolve<IStartableServiceBus>();
        }

        private IStartableServiceBus CreateSubscriber()
        {
            var busConfig = new BusConfigurationSection();
            busConfig.Bus.Endpoint = this.TestQueueUri2.Uri.ToString();
            busConfig.Bus.ThreadCount = 1;
            busConfig.Bus.NumberOfRetries = 1;
            busConfig.MessageOwners.Add(new MessageOwnerElement()
            {
                Name = this.GetType().FullName,
                Endpoint = this.TestQueueUri.Uri.ToString()
            });

            this.subscriberContainer = new WindsorContainer();
            new RhinoServiceBusConfiguration()
                .UseConfiguration(busConfig)
                .UseCastleWindsor(subscriberContainer)
                .Configure();
            return subscriberContainer.Resolve<IStartableServiceBus>();
        }

        private static void WaitForQueueEmpty(IStartableServiceBus publisherBus)
        {
            using (var publisherQueue = MsmqUtil.GetQueuePath(publisherBus.Endpoint).Open())
            {
                Stopwatch timeout = new Stopwatch();
                timeout.Start();

                while (true)
                {
                    try
                    {
                        publisherQueue.Peek(TimeSpan.FromMilliseconds(50));
                    }
                    catch (MessageQueueException mqx)
                    {
                        if (mqx.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            break;
                        }
                        else
                        {
                            throw;
                        }
                    }

                    if (5000 < timeout.ElapsedMilliseconds)
                    {
                        throw new TimeoutException("Publisher did not drain its queue in the expected time.");
                    }
                };
            }
        }

        public class MessageA
        {
        }

        public class MessageB
        {
        }

        public class Subscriber : OccasionalConsumerOf<MessageA>, OccasionalConsumerOf<MessageB>
        {
            private List<MessageA> receivedAs = new List<MessageA>();

            private List<MessageB> receivedBs = new List<MessageB>();

            public IEnumerable<MessageA> ReceivedAs
            {
                get
                {
                    return receivedAs;
                }
            }

            public IEnumerable<MessageB> ReceivedBs
            {
                get
                {
                    return receivedBs;
                }
            }

            public void Consume(MessageA message)
            {
                this.receivedAs.Add(message);
            }

            public void Consume(MessageB message)
            {
                this.receivedBs.Add(message);
            }
        }

        [Fact]
        public void Subscriber_should_have_consumers_registered_for_both_messages()
        {
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                subscriberBus.AddInstanceSubscription(subscriberInstance);

                var concreteSubscriberBus = (DefaultServiceBus)subscriberBus;

                var subscribersForA = concreteSubscriberBus.GatherConsumers(new CurrentMessageInformation() { Message = new MessageA() });
                Assert.Equal(1, subscribersForA.Length);
                Assert.Equal(subscriberInstance, subscribersForA[0]);

                var subscribersForB = concreteSubscriberBus.GatherConsumers(new CurrentMessageInformation() { Message = new MessageB() });
                Assert.Equal(1, subscribersForB.Length);
                Assert.Equal(subscriberInstance, subscribersForB[0]);
            }
        }

        [Fact]
        public void Publishing_should_work_for_both_messages()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                subscriberBus.AddInstanceSubscription(subscriberInstance);

                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);

                publisherBus.Publish(new MessageA());
                publisherBus.Publish(new MessageB());
                WaitForQueueEmpty(subscriberBus);

                Assert.Equal(1, subscriberInstance.ReceivedAs.Count());
                Assert.Equal(1, subscriberInstance.ReceivedBs.Count());
            }
        }

        [Fact]
        public void Subscribing_and_unsubscribing_should_result_in_no_subscriptions_returned_by_MSMQ_storage()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                this.TestSubscriptionStorage(publisherBus, subscriberBus);
            }
        }

        [Fact]
        public void Subscribing_and_unsubscribing_should_result_in_no_subscriptions_returned_by_PHT_storage()
        {
            using (var publisherBus = this.CreatePublisherWithPHT())
            using (var subscriberBus = this.CreateSubscriber())
            {
                this.TestSubscriptionStorage(publisherBus, subscriberBus);
            }
        }

        private void TestSubscriptionStorage(IStartableServiceBus publisherBus, IStartableServiceBus subscriberBus)
        {
            var subscriberInstance = new Subscriber();

            subscriberBus.Start();
            var subscriptionToken = subscriberBus.AddInstanceSubscription(subscriberInstance);

            publisherBus.Start();
            WaitForQueueEmpty(publisherBus);

            var subscriptionStorage = this.publisherContainer.Resolve<ISubscriptionStorage>();
            var subscribersForA = subscriptionStorage.GetSubscriptionsFor(typeof(MessageA));
            Assert.Equal(1, subscribersForA.Count());
            var subscribersForB = subscriptionStorage.GetSubscriptionsFor(typeof(MessageB));
            Assert.Equal(1, subscribersForB.Count());

            subscriptionToken.Dispose();
            WaitForQueueEmpty(publisherBus);

            subscribersForA = subscriptionStorage.GetSubscriptionsFor(typeof(MessageA));
            Assert.Equal(0, subscribersForA.Count());
            subscribersForB = subscriptionStorage.GetSubscriptionsFor(typeof(MessageB));
            Assert.Equal(0, subscribersForB.Count());
        }

        [Fact]
        public void Subscribing_and_unsubscribing_should_result_in_no_subscriptions_in_MSMQ_queue()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                var subscriptionToken = subscriberBus.AddInstanceSubscription(subscriberInstance);

                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);

                var subscriptionMessages = this.subscriptions.GetAllMessages();
                Assert.Equal(2, subscriptionMessages.Length);

                subscriptionToken.Dispose();
                WaitForQueueEmpty(publisherBus);

                subscriptionMessages = this.subscriptions.GetAllMessages();
                Assert.Equal(0, subscriptionMessages.Length);
            }
        }

        [Fact]
        public void Subscribing_and_unsubscribing_should_result_in_no_subscriptions_in_PHT_database()
        {
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                var subscriptionToken = subscriberBus.AddInstanceSubscription(subscriberInstance);

                using (var publisherBus = this.CreatePublisherWithPHT())
                {
                    publisherBus.Start();
                    WaitForQueueEmpty(publisherBus);
                }

                Assert.Equal(2, GetPhtSubscriptionsCount());

                using (var publisherBus = this.CreatePublisherWithPHT())
                {
                    subscriptionToken.Dispose();
                    WaitForQueueEmpty(publisherBus);
                }
            }

            Assert.Equal(0, GetPhtSubscriptionsCount());
        }

        private static int GetPhtSubscriptionsCount()
        {
            int? subscriptionsCount = null;
            using (var pht = new PersistentHashTable.PersistentHashTable(PhtSubscriptionsPath))
            {
                pht.Initialize();
                pht.Batch(
                    action =>
                    {
                        var items = action.GetItems(
                            new PersistentHashTable.GetItemsRequest()
                            {
                                Key = "subscriptions"
                            });
                        subscriptionsCount = items.Length;
                    });
            }
            return subscriptionsCount.Value;
        }

        [Fact]
        public void Publishing_should_not_work_for_both_messages_after_unsubscribing()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                var subscriptionToken = subscriberBus.AddInstanceSubscription(subscriberInstance);

                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);
                subscriptionToken.Dispose();

                Assert.Throws<MessagePublicationException>(() => publisherBus.Publish(new MessageA()));
                Assert.Throws<MessagePublicationException>(() => publisherBus.Publish(new MessageB()));

                WaitForQueueEmpty(subscriberBus);

                Assert.Equal(0, subscriberInstance.ReceivedAs.Count());
                Assert.Equal(0, subscriberInstance.ReceivedBs.Count());
            }
        }

        [Fact]
        public void Publishing_should_not_work_for_both_messages_after_unsubscribing_and_restarting_publisher()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                var subscriberInstance = new Subscriber();

                subscriberBus.Start();
                var subscriptionToken = subscriberBus.AddInstanceSubscription(subscriberInstance);

                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);
                subscriptionToken.Dispose();
                WaitForQueueEmpty(publisherBus);
            }

            using (var publisherBus = this.CreatePublisher())
            {
                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);

                Assert.Throws<MessagePublicationException>(() => publisherBus.Publish(new MessageA()));
                Assert.Throws<MessagePublicationException>(() => publisherBus.Publish(new MessageB()));
            }
        }

        [Fact]
        public void Publishing_should_work_for_both_messages_after_subscribing_again()
        {
            using (var publisherBus = this.CreatePublisher())
            using (var subscriberBus = this.CreateSubscriber())
            {
                var firstSubscriberInstance = new Subscriber();

                subscriberBus.Start();
                var firstSubscriptionToken = subscriberBus.AddInstanceSubscription(firstSubscriberInstance);

                publisherBus.Start();
                WaitForQueueEmpty(publisherBus);
                firstSubscriptionToken.Dispose();

                var secondSubscriberInstance = new Subscriber();
                subscriberBus.AddInstanceSubscription(secondSubscriberInstance);
                WaitForQueueEmpty(publisherBus);

                publisherBus.Publish(new MessageA());
                publisherBus.Publish(new MessageB());
                WaitForQueueEmpty(subscriberBus);

                Assert.Equal(1, secondSubscriberInstance.ReceivedAs.Count());
                Assert.Equal(1, secondSubscriberInstance.ReceivedBs.Count());
            }
        }

        [Fact]
        public void Publishing_should_work_for_both_messages_after_unsubscribing_subscribing_again_and_restarting_publisher()
        {
            using (var subscriberBus = this.CreateSubscriber())
            {
                var firstSubscriberInstance = new Subscriber();
                subscriberBus.Start();

                using (var publisherBus = this.CreatePublisher())
                {
                    var firstSubscriptionToken = subscriberBus.AddInstanceSubscription(firstSubscriberInstance);

                    publisherBus.Start();
                    WaitForQueueEmpty(publisherBus);

                    firstSubscriptionToken.Dispose();
                }

                var secondSubscriberInstance = new Subscriber();
                subscriberBus.AddInstanceSubscription(secondSubscriberInstance);

                using (var publisherBus = this.CreatePublisher())
                {
                    publisherBus.Start();
                    WaitForQueueEmpty(publisherBus);

                    publisherBus.Publish(new MessageA());
                    publisherBus.Publish(new MessageB());
                }

                WaitForQueueEmpty(subscriberBus);

                Assert.Equal(1, secondSubscriberInstance.ReceivedAs.Count());
                Assert.Equal(1, secondSubscriberInstance.ReceivedBs.Count());
                Assert.Equal(0, firstSubscriberInstance.ReceivedAs.Count());
                Assert.Equal(0, firstSubscriberInstance.ReceivedBs.Count());
            }
        }
    }
}
