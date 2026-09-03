using Mediana.Internal;
using Mediana.Dispatch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Mediana;

/// <summary>Mediator DI integration.</summary>
public static class MedianaServiceCollectionExtensions
{
    /// <summary>
    /// Registers IMediator. Handlers are registered as scoped (or singleton with UseSingletonHandlers).
    /// cfg.UseGeneratedRegistrar() (Mediana.Generators) enables source-gen registration without reflection.
    /// </summary>
    public static IServiceCollection AddMediana(
        this IServiceCollection services,
        Action<MedianaConfiguration> configure)
    {
        // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        Guard.NotNull(services, nameof(services));
        // Stryker disable once statement: fallback/perf-equivalent (see CallSiteBranchTests: fast/slow paths are identical)
        Guard.NotNull(configure, nameof(configure));

        var configuration = new MedianaConfiguration();
        configure(configuration);

        // Handler DI registration: lifetime per configuration policy.
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

        // Mediator is scoped: resolves handlers from the current scope (scoped-dependency semantics).
        services.TryAddScoped<IMediator>(sp => new Mediator(registry, sp));
        services.AddSingleton(registry);
        return services;
    }
}
