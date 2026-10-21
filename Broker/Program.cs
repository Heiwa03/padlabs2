using Broker;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddGrpc();
builder.Services.AddSingleton<MessageStore>();
builder.Services.AddSingleton<TopicRegistry>();
builder.Services.AddHostedService<DispatchWorker>();

var app = builder.Build();
app.MapGrpcService<BrokerService>();
app.MapGet("/", () => "gRPC message broker");
app.Run();
