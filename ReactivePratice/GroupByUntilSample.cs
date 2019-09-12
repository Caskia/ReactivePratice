using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace ReactivePratice
{
    public class GroupByUntilSample
    {
        public void Start()
        {
            var subject = Subject.Synchronize(new Subject<Message>());

            Observable
                .Interval(TimeSpan.FromMilliseconds(10))
                .Subscribe(x => Process(subject));

            Observable
                .Interval(TimeSpan.FromMilliseconds(10))
                .Subscribe(x => Process(subject));

            Observable
                .Interval(TimeSpan.FromMilliseconds(10))
                .Subscribe(x => Process(subject));

            Observable
                .Interval(TimeSpan.FromMilliseconds(10))
                .Subscribe(x => Process(subject));

            var timeObserver = subject
                .GroupByUntil(g => g.IdempotentContentId, g => g.Buffer(TimeSpan.FromSeconds(5), 5))
                .Subscribe(group =>
                {
                    group.Count()
                     .Where(groupCount => groupCount != 5)
                     .Subscribe(groupCount => Console.WriteLine($"error processing group {group.Key} count {groupCount}"));

                    group.ToList()
                    .Select(messages => Observable.FromAsync(async () =>
                    {
                        var groupedMessages = messages.GroupBy
                       (
                           m => m.IdempotentContentId,
                           m => m,
                           (key, m) => new GroupedMessage { IdempotentContentId = key, Count = m.Count(), Content = string.Join(",", m.Select(t => t.Content)) }
                       );

                        if (groupedMessages.Count() != 1)
                        {
                            Console.WriteLine($"processing group error count {group.Key}");
                        }

                        await ProcessGroupedMessage(groupedMessages.FirstOrDefault());
                    }))
                    .Concat()
                    .Subscribe();
                });

            //Observable
            //    .Range(0, 10)
            //    .Subscribe(x => Process(subject));

            //Observable
            //    .Range(0, 10)
            //    .Subscribe(x => Process(subject));

            //Observable
            //    .Range(0, 10)
            //    .Subscribe(x => Process(subject));

            //Observable
            //    .Range(0, 10)
            //    .Subscribe(x => Process(subject));
        }

        private void Process(ISubject<Message> subject)
        {
            var random = new Random();

            subject.OnNext(new Message()
            {
                Content = Guid.NewGuid().ToString(),
                IdempotentContentId = random.Next(10).ToString()
            });
        }

        private async Task ProcessGroupedMessage(GroupedMessage groupedMessage)
        {
            Console.WriteLine($"start processing group [{groupedMessage.IdempotentContentId}] count[{groupedMessage.Count}] content[{groupedMessage.Content}]");

            await Task.Delay(1000);

            Console.WriteLine($"complete processing group [{groupedMessage.IdempotentContentId}] count[{groupedMessage.Count}] content[{groupedMessage.Content}]");
        }

        public class GroupedMessage : Message
        {
            public int Count { get; set; }
        }

        public class GroupObserver : IObserver<IGroupedObservable<string, Message>>
        {
            public void OnCompleted()
            {
                Console.WriteLine($"processing group complete");
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnNext(IGroupedObservable<string, Message> group)
            {
                group.Count()
                 .Where(c => c != 5)
                 .Subscribe(c => Console.WriteLine($"error processing group {group.Key} count {c}"));

                group.ToList().Subscribe(ms =>
                {
                    Console.WriteLine($"start processing group {group.Key}");

                    var groupedMessages = ms.GroupBy
                    (
                        m => m.IdempotentContentId,
                        m => m,
                        (key, m) => new GroupedMessage { IdempotentContentId = key, Count = m.Count(), Content = string.Join(",", m.Select(t => t.Content)) }
                    );

                    if (groupedMessages.Count() > 1)
                    {
                        Console.WriteLine($"processing group error count {group.Key}");
                    }

                    Console.WriteLine($"process group {group.Key} content{groupedMessages.FirstOrDefault().Content}");

                    Console.WriteLine($"complete processing group {group.Key}");
                });
            }
        }

        public class Message
        {
            public string Content { get; set; }

            public string IdempotentContentId { get; set; }
        }
    }
}