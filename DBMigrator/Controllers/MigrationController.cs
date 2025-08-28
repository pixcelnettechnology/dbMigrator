using DBMigrator.Models;
using DBMigrator.Services;
using Microsoft.AspNetCore.Mvc;

namespace DBMigrator.Controllers
{
    public class MigrationController : Controller
    {
        private readonly MigrationService _svc;
        private readonly ILogger<MigrationController> _logger;

        public MigrationController(MigrationService svc, ILogger<MigrationController> logger)
        {
            _svc = svc; _logger = logger;
        }

        [HttpGet]
        public IActionResult Index() => View(new MigrationRequest());

        [HttpPost]
        public async Task<IActionResult> Summary(MigrationRequest req)
        {
            // If user provided CSV text in single input, normalize:
            if (Request.Form.TryGetValue("Tables", out var csv) && !string.IsNullOrWhiteSpace(csv))
            {
                if (!string.IsNullOrWhiteSpace(req.Tables))
                {
                    req.TablesToMigrate = [.. req.Tables.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];
                }
            }

            if (string.IsNullOrWhiteSpace(req.SourceConnectionString) || string.IsNullOrWhiteSpace(req.TargetConnectionString))
            {
                ModelState.AddModelError(string.Empty, "Source and target connection strings are required.");
                return View("Index", req);
            }

            var summary = await _svc.PreviewAsync(req);
            TempData["MigrationRequest"] = System.Text.Json.JsonSerializer.Serialize(req);
            return View( summary);
        }

        [HttpPost]
        public async Task<IActionResult> StartMigration()
        {
            if (!TempData.TryGetValue("MigrationRequest", out var ser) || ser == null) return RedirectToAction("Index");
            var req = System.Text.Json.JsonSerializer.Deserialize<MigrationRequest>(ser.ToString()!)!;
            var report = await _svc.MigrateAsync(req);
            TempData["LastReport"] = System.Text.Json.JsonSerializer.Serialize(report);
            return View("Report", report);
        }

        public IActionResult Report()
        {
            if (!TempData.TryGetValue("LastReport", out var ser) || ser == null) return RedirectToAction("Index");
            var report = System.Text.Json.JsonSerializer.Deserialize<MigrationReport>(ser.ToString()!)!;
            return View(report);
        }
    }
}