using Mediana.Internal;
using Mediana.Dispatch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Mediana;

/// <summary>DI-интеграция медиатора.</summary>
public static class MedianaServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует IMediator. Хендлеры регистрируются как scoped (или singleton при UseSingletonHandlers).
    /// cfg.UseGeneratedRegistrar() (Mediana.Generators) подключает source-gen регистрацию без рефлексии.
    /// </summary>
    public static IServiceCollection AddMediana(
        this IServiceCollection services,
        Action<MedianaConfiguration> configure)
    {
        Guard.NotNull(services, nameof(services));
        Guard.NotNull(configure, nameof(configure));

        var configuration = new MedianaConfiguration();
        configure(configuration);

        // Регистрация хендлеров в DI: lifetime по политике конфигурации.
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

        // Mediator — scoped: резолвит хендлеры из текущего scope (семантика scoped-зависимостей).
        services.TryAddScoped<IMediator>(sp => new Mediator(registry, sp));
        services.AddSingleton(registry);
        return services;
    }
}
