using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace CosmosDBTest
{
    internal class Program
    {
        private const string EndpointUrl = "";
        private const string PrimaryKey = "";
        private DocumentClient client;

        public class Family
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string LastName { get; set; }
            public Parent[] Parents { get; set; }
            public Child[] Children { get; set; }
            public Address Address { get; set; }
            public bool IsRegistered { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class Parent
        {
            public string FamilyName { get; set; }
            public string FirstName { get; set; }
        }
        public class Child
        {
            public string FamilyName { get; set; }
            public string FirstName { get; set; }
            public string Gender { get; set; }
            public int Grade { get; set; }
            public Pet[] Pets { get; set; }
        }
        public class Pet
        {
            public string GivenName { get; set; }
        }
        public class Address
        {
            public string State { get; set; }
            public string County { get; set; }
            public string City { get; set; }
        }

        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            Console.WriteLine("Press any key to continue ... \n\n");
            Console.ReadKey();
        }

        // ================ Create Calls ================
        private async Task CreateDatabaseIfNotExists(string databaseName)
        {
            await this.client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });
            WriteToConsoleAndPromptToContinue("Created Database: {0}", databaseName);
        }

        private async Task CreateCollectionIfNotExists(string databaseName, string collectionName, string partitionKey = "")
        {
            await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection
                {
                    Id = collectionName,
                    PartitionKey = new PartitionKeyDefinition
                    {
                        Paths = new Collection<string> { partitionKey }
                    }
                });
            WriteToConsoleAndPromptToContinue("Created Document Collection: {0}", collectionName);
        }

        private async Task CreateDocumentIfNotExists(string databaseName, string collectionName, Family family)
        {
            try
            {
                await this.client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(databaseName, collectionName, family.Id),
                    new RequestOptions
                    {
                        PartitionKey = new PartitionKey(family.LastName)
                    });

                this.WriteToConsoleAndPromptToContinue("Found Document: {0}", family.Id);
            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), family);
                    this.WriteToConsoleAndPromptToContinue("Created Document: {0}", family.Id);
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task UpsertDocument(string databaseName, string collectionName, Family family)
        {
            try
            {
                await this.client.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), family);
                await this.client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(databaseName, collectionName, family.Id),
                    new RequestOptions
                    {
                        PartitionKey = new PartitionKey(family.LastName)
                    });

                this.WriteToConsoleAndPromptToContinue("Upserted Document: {0}", family.Id);
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
                throw;
            }
        }

        // ================ Get Calls ================
        private void ExecuteQueries(string databaseName, string collectionName)
        {
            #region In-Partition Query
            Console.WriteLine("Executing In-Partition Query");
            IQueryable<Family> singlePartitionQueryResult = this.client.CreateDocumentQuery<Family>(
                        UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                        new FeedOptions { MaxItemCount = -1 })
                    //.Where(f => f.LastName == "Andersen");
                    .Where(f => f.IsRegistered == false && f.LastName == "Stanford");

            // The query is executed synchronously here, but can also be executed asynchronously via the IDocumentQuery<T> interface
            foreach (Family family in singlePartitionQueryResult)
            {
                WriteToConsoleAndPromptToContinue("\tRead {0}", family);
            }

            #endregion

            #region Cross-Partition Query
            // Querying for data.Property1 = value && data.Property2 = value ===
            Console.WriteLine("Executing Cross-Partition Query");
            IQueryable<Family> CrossPartitionQueryResult = this.client.CreateDocumentQuery<Family>(
                        UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                        new FeedOptions { EnableCrossPartitionQuery = true, MaxItemCount = -1 })
                    .Where(f => f.IsRegistered == false && f.Children.Count() > 1);

            // The query is executed synchronously here, but can also be executed asynchronously via the IDocumentQuery<T> interface
            foreach (Family family in CrossPartitionQueryResult)
            {
                WriteToConsoleAndPromptToContinue("\tRead {0}", family);
            }

            #endregion

            #region Direct SQL Query
            Console.WriteLine("Running direct SQL query...");
            IQueryable<Family> familyQueryInSql = this.client.CreateDocumentQuery<Family>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                    "SELECT * FROM Family WHERE Family.LastName = 'Andersen'",
                    new FeedOptions { MaxItemCount = -1 });

            foreach (Family family in familyQueryInSql)
            {
                WriteToConsoleAndPromptToContinue("\tRead {0}", family);
            }

            #endregion
        }

        private async Task GetAllDocumentsInCollection(string databaseName, string collectionName)
        {
            WriteToConsoleAndPromptToContinue("Getting All Documents in Collection");
            string continuationToken = null;
            do
            {
                FeedResponse<dynamic> feed = await this.client.ReadDocumentFeedAsync(
                     UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                     new FeedOptions { MaxItemCount = 1, RequestContinuation = continuationToken });

                continuationToken = feed.ResponseContinuation;
                //Console.WriteLine("New ContinuationToken {0}", continuationToken);
                foreach (Document document in feed)
                {
                    Console.WriteLine(document);
                }
            }
            while (continuationToken != null);
        }

        // ================ Update Calls ================
        private async Task ReplaceDocument(string databaseName, string collectionName, string familyName, Family updatedFamily)
        {
            await this.client.ReadDocumentAsync(
                UriFactory.CreateDocumentUri(databaseName, collectionName, updatedFamily.Id),
                new RequestOptions
                {
                    PartitionKey = new PartitionKey(updatedFamily.LastName)
                });
            this.WriteToConsoleAndPromptToContinue("Found Document to Replace {0}", updatedFamily.Id);

            await this.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(databaseName, collectionName, familyName), updatedFamily);
            this.WriteToConsoleAndPromptToContinue("Replaced Document {0}", familyName);
        }

        // ================ Delete Calls ================
        private async Task DeleteDocument(string databaseName, string collectionName, Family family)
        {
            await this.client.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(databaseName, collectionName, family.Id),
                new RequestOptions
                {
                    PartitionKey = new PartitionKey(family.LastName)
                });
            WriteToConsoleAndPromptToContinue("Deleted Document {0}", family.Id);
        }

        private async Task DeleteDatabase(string databaseName)
        {
            await this.client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
            WriteToConsoleAndPromptToContinue("Deleted Database: {0}", databaseName);
        }

        private async Task StartDemo()
        {
            this.client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);

            // 1. Create Database
            await this.CreateDatabaseIfNotExists("FamilyDB");

            // 2. Create Document Collection
            await this.CreateCollectionIfNotExists("FamilyDB", "FamilyCollection", "/LastName");

            // 3. Add Documents to Collection
            Family andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                        new Parent { FirstName = "Thomas" },
                        new Parent { FirstName = "Mary Kay" }
                },
                Children = new Child[]
                {
                        new Child
                        {
                                FirstName = "Henriette Thaulow",
                                Gender = "female",
                                Grade = 5,
                                Pets = new Pet[]
                                {
                                        new Pet { GivenName = "Fluffy" }
                                }
                        }
                },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = true
            };
            await this.CreateDocumentIfNotExists("FamilyDB", "FamilyCollection", andersenFamily);

            Family wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new Parent[]
                    {
                new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                new Parent { FamilyName = "Miller", FirstName = "Ben" }
                    },
                Children = new Child[]
                    {
                new Child
                {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                                new Pet { GivenName = "Goofy" },
                                new Pet { GivenName = "Shadow" }
                        }
                },
                new Child
                {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Grade = 1
                }
                    },
                Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                IsRegistered = false
            };
            await this.CreateDocumentIfNotExists("FamilyDB", "FamilyCollection", wakefieldFamily);

            // 4. Upsert Documents
            Family StanfordFamily1 = new Family
            {
                Id = "Stanford.9",
                LastName = "Stanford",
                Parents = new Parent[]
                    {
                new Parent { FamilyName = "Stanford", FirstName = "ellen" },
                new Parent { FamilyName = "Stanford", FirstName = "Ben" }
                    },
                Children = new Child[]
                    {
                new Child
                {
                        FamilyName = "webster",
                        FirstName = "Martina",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                                new Pet { GivenName = "Scooby" }
                        }
                },
                new Child
                {
                        FamilyName = "Stanford",
                        FirstName = "lauren",
                        Gender = "female",
                        Grade = 1
                }
                    },
                Address = new Address { State = "AB", County = "King", City = "Calgary" },
                IsRegistered = false
            };
            await this.UpsertDocument("FamilyDB", "FamilyCollection", StanfordFamily1);

            Family StanfordFamily2 = new Family
            {
                Id = "Stanford.11",
                LastName = "Stanford",
                Parents = new Parent[]
                    {
                new Parent { FamilyName = "Stanford", FirstName = "Brianna" },
                new Parent { FamilyName = "Stanford", FirstName = "Albert" }
                    },
                Children = new Child[]
                    {
                new Child
                {
                        FamilyName = "Stanford",
                        FirstName = "Timothy",
                        Gender = "male",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                                new Pet { GivenName = "roadrunner" }
                        }
                }
            },
                Address = new Address { State = "OR", County = "Queen", City = "Portland" },
                IsRegistered = true
            };
            await this.UpsertDocument("FamilyDB", "FamilyCollection", StanfordFamily2);

            // 5. Query Documents
            this.ExecuteQueries("FamilyDB", "FamilyCollection");

            // 6. Get All Documents in Collection
            await this.GetAllDocumentsInCollection("FamilyDB", "FamilyCollection");

            // 7. Replace Document
            andersenFamily.Children[0].Grade = 6;
            await this.ReplaceDocument("FamilyDB", "FamilyCollection", "Andersen.1", andersenFamily);

            // 8. Delete Documents
            await this.DeleteDocument("FamilyDB", "FamilyCollection", andersenFamily);
            await this.DeleteDocument("FamilyDB", "FamilyCollection", wakefieldFamily);
            await this.DeleteDocument("FamilyDB", "FamilyCollection", StanfordFamily1);
            await this.DeleteDocument("FamilyDB", "FamilyCollection", StanfordFamily2);

            // 9. Delete Database
            await this.DeleteDatabase("FamilyDB");
        }

        private static void Main(string[] args)
        {
            try
            {
                Program p = new Program();
                p.StartDemo().Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
    }
}
