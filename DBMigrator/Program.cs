using DBMigrator.Services;
using DBMigrator.Providers;

var builder = WebApplication.CreateBuilder(args);

// 🔹 Configure Kestrel request limits here
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestHeadersTotalSize = 64 * 1024; // 64 KB
    options.Limits.MaxRequestBufferSize = 104857600;       // 100 MB
});

// Add services and DI
builder.Services.AddControllersWithViews();
builder.Services.AddLogging();

builder.Services.AddSingleton<ProviderFactory>();            // factory to create provider instances
builder.Services.AddScoped<MigrationService>();              // migration orchestrator

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseStaticFiles();
app.UseRouting();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Migration}/{action=Index}/{id?}");

app.Run();