using Mediana.Internal;
using Mediana.Dispatch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Mediana;

/// <summary>DI-.</summary>
public static class MedianaServiceCollectionExtensions
{
    /// <summary>
    /// IMediator. scoped (singleton UseSingletonHandlers)
    /// cfg.UseGeneratedRegistrar() (Mediana.Generators) source-gen
    /// </summary>
    public static IServiceCollection AddMediana(
        this IServiceCollection services,
        Action<MedianaConfiguration> configure)
    {
        // Stryker disable once statement: fallback/perf-(. CallSiteBranchTests: fast/slow )
        Guard.NotNull(services, nameof(services));
        // Stryker disable once statement: fallback/perf-(. CallSiteBranchTests: fast/slow )
        Guard.NotNull(configure, nameof(configure));

        var configuration = new MedianaConfiguration();
        configure(configuration);

        // DI: lifetime
        foreach (var handlerType in configuration.HandlerTypes)
        {
            if (configuration.IsSingleton)
            {
                services.TryAddSingleton(handlerType, handlerType);
            }
            else
            {
                services.TryAddScoped(handlerType, handlerType);
            }
        }

        var registry = configuration.Freeze();

        // Mediator — scoped: scope (scoped-)
        services.TryAddScoped<IMediator>(sp => new Mediator(registry, sp));
        services.AddSingleton(registry);
        return services;
    }
}
