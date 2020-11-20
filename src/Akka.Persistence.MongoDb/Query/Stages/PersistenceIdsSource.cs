using Akka.Actor;
using Akka.Persistence.MongoDb.Journal;
using Akka.Streams;
using Akka.Streams.Stage;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Akka.Persistence.MongoDb.Query.Stages
{
    internal class PersistenceIdsSource : GraphStage<SourceShape<string>>
    {
        private IMongoCollection<JournalEntry> _journalCollection;
        private IMongoCollection<MetadataEntry> _metadataCollection;
        private ExtendedActorSystem _system;
        private PipelineDefinition<ChangeStreamDocument<JournalEntry>, ChangeStreamDocument<JournalEntry>> _pipeline;
        private ChangeStreamOptions _changeStreamOptions;
        public PersistenceIdsSource(IMongoCollection<JournalEntry> journalCollection, IMongoCollection<MetadataEntry> metadataCollection, ExtendedActorSystem system, long offSet)
        {
            _journalCollection = journalCollection;
            _metadataCollection = metadataCollection;
            _system = system;
           _pipeline =  new EmptyPipelineDefinition<ChangeStreamDocument<JournalEntry>>()
            .Match(x => x.OperationType == ChangeStreamOperationType.Insert);
            //we are only interested in insert
            /*_changeStreamOptions = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                StartAtOperationTime = new BsonTimestamp(offSet)               
            };*/
        }
        public Outlet<string> Outlet { get; } = new Outlet<string>(nameof(PersistenceIdsSource));

        public override SourceShape<string> Shape => new SourceShape<string>(Outlet);

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return new PersistenceIdsLogic(_journalCollection, _metadataCollection, _pipeline, _system, Outlet, Shape);
        }
        private class PersistenceIdsLogic : GraphStageLogic
        {
            private bool _start = true;
            private long _index = 0;
            private readonly Queue<string> _buffer = new Queue<string>();
            private bool _downstreamWaiting = false;

            private readonly Outlet<string> _outlet;
            private readonly IMongoCollection<JournalEntry> _journalCollection;
            private readonly IMongoCollection<MetadataEntry> _metadataCollection;
            readonly PipelineDefinition<ChangeStreamDocument<JournalEntry>, ChangeStreamDocument<JournalEntry>> _pipeline;


            public PersistenceIdsLogic(IMongoCollection<JournalEntry> journalCollection, IMongoCollection<MetadataEntry> metadataCollection, PipelineDefinition<ChangeStreamDocument<JournalEntry>, ChangeStreamDocument<JournalEntry>> pipeline, ExtendedActorSystem system, Outlet<string> outlet, Shape shape) : base(shape)
            {
                _journalCollection = journalCollection;
                _metadataCollection = metadataCollection;
                _outlet = outlet;
                SetHandler(outlet, onPull: () =>
                {
                    _downstreamWaiting = true;
                    if (_buffer.Count == 0 && (_start || _index > 0))
                    {
                        var callback = GetAsyncCallback<(IEnumerable<string> Ids, long lastOrdering)>(doc =>
                        {
                            // save the index for further initialization if needed
                            _index = doc.lastOrdering;

                            // it is not the start anymore
                            _start = false;

                            // enqueue received data
                            try
                            {
                                foreach(var data in doc.Ids)
                                    _buffer.Enqueue(data);
                            }
                            catch (Exception e)
                            {
                                Log.Error(e, "Error while querying persistence identifiers");
                                FailStage(e);
                            }

                            // deliver element
                            Deliver();
                        });
                        callback(SelectAllPersistenceIds());
                    }
                    else if (_buffer.Count == 0)
                    {
                        // wait for asynchornous notification and mark dowstream
                        // as waiting for data
                    }
                    else
                    {
                        Deliver();
                    }
                });
            }

            public override void PreStart()
            {
                var callback = GetAsyncCallback<ChangeStreamDocument<JournalEntry>>(data =>
                {
                    Log.Debug("Message received");

                    // enqueue the element
                    _buffer.Enqueue(data.FullDocument.PersistenceId);

                    // deliver element
                    Deliver();
                });
                /*
                 using (var cursor = await collection.WatchAsync())
                {
                    await cursor.ForEachAsync(change =>
                    {
                        // process change event
                    });
                }
                 */
                using (var cursor = _journalCollection.Watch(_pipeline))
                {
                    foreach (var change in cursor.ToEnumerable())
                    {
                        callback.Invoke(change);
                    }
                }
            }

            public override void PostStop()
            {
               
            }

            private void Deliver()
            {
                if (_downstreamWaiting)
                {
                    _downstreamWaiting = false;
                    var elem = _buffer.Dequeue();
                    Push(_outlet, elem);
                }
            }
            private (IEnumerable<string> Ids, long LastOrdering) SelectAllPersistenceIds()
            {
                var lastOrdering = GetHighestOrdering();
                var ids = GetAllPersistenceIds();
                return (ids, lastOrdering);
            }
            private IEnumerable<string> GetAllPersistenceIds()
            {
                return _metadataCollection.AsQueryable().Select(x=> x.PersistenceId).ToList();
            }

            private long GetHighestOrdering()
            {
                return _journalCollection.AsQueryable()
                        .Select(je => je.Ordering)
                        .Distinct().Max().Value;
            }
        }
    }
}
