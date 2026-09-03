using System.Reflection;
using System.Diagnostics.CodeAnalysis;
using MediatR;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.MediatR;

/// <summary>
/// Адаптер MediatR 12+/14+: существующие MediatR-хендлеры выполняются через Mediana без изменений
/// (D1). Мост сканирует сборки, резолвит хендлеры из DI и диспатчит с семантикой Mediana
/// (ValueTask, исключения как есть). Behaviors Mediana применяются к Mediana-native сообщениям;
/// bridge MediatR-behaviors зафиксирован в docs/QUESTIONS.md (Q8).
/// </summary>
public sealed class MediatRBridge
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Dictionary<Type, HandlerKind> _kinds = [];

    public MediatRBridge(IServiceProvider serviceProvider, params Assembly[] assemblies)
    {
        _serviceProvider = serviceProvider;
        foreach (var assembly in assemblies)
        {
            Scan(assembly);
        }
    }

    private enum HandlerKind
    {
        RequestWithResponse,
        RequestVoid,
        Notification,
    }

    [RequiresUnreferencedCode("Сканирование MediatR-хендлеров: для AOT регистрируйте вручную.")]
    private void Scan(Assembly assembly)
    {
        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            // H-7 fix: plugin-сценарии — частично загруженные сборки
            types = ex.Types.Where(t => t is not null).Select(t => t!).ToArray();
        }

        foreach (var type in types)
        {
            if (type is not { IsAbstract: false, IsInterface: false, IsGenericTypeDefinition: false })
            {
                continue;
            }

            foreach (var iface in type.GetInterfaces())
            {
                if (!iface.IsGenericType)
                {
                    continue;
                }

                var def = iface.GetGenericTypeDefinition();
                if (def == typeof(global::MediatR.IRequestHandler<,>))
                {
                    _kinds[iface.GetGenericArguments()[0]] = HandlerKind.RequestWithResponse;
                    break;
                }

                if (def == typeof(global::MediatR.IRequestHandler<>))
                {
                    _kinds[iface.GetGenericArguments()[0]] = HandlerKind.RequestVoid;
                    break;
                }

                if (def == typeof(global::MediatR.INotificationHandler<>))
                {
                    _kinds[iface.GetGenericArguments()[0]] = HandlerKind.Notification;
                    break;
                }
            }
        }
    }

    // H-8/M-13/M-15 fix: кэш типизированных делегатов вместо reflection на каждый вызов
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(Type RequestType, Type ResponseType), Func<object, object, CancellationToken, Task>> HandleCache = new();

    /// <summary>Выполнить MediatR-запрос через Mediana-мост (кэш делегатов, исключения as-is).</summary>
    [RequiresDynamicCode("MakeGenericType: для AOT регистрируйте хендлеры явно.")]
    [RequiresUnreferencedCode("Рефлексивный резолв хендлеров: для trimming — явная регистрация.")]
    public async ValueTask<TResponse> Send<TResponse>(global::MediatR.IRequest<TResponse> request, CancellationToken cancellationToken = default)
    {
        Guard(request);
        var requestType = request.GetType();
        var handlerType = typeof(global::MediatR.IRequestHandler<,>).MakeGenericType(requestType, typeof(TResponse));
        var handler = _serviceProvider.GetService(handlerType);
        if (handler is null)
        {
            throw new MediatorConfigurationException(
                "No MediatR handler registered for " + requestType + ". " +
                "Register IRequestHandler<" + requestType.Name + ", TResponse> in DI.");
        }

        var invoke = HandleCache.GetOrAdd((requestType, typeof(TResponse)), static key =>
        {
            // M-15 fix: точный overload через параметр-типы
            var closedInterface = typeof(global::MediatR.IRequestHandler<,>).MakeGenericType(key.RequestType, key.ResponseType);
            var method = closedInterface.GetMethod("Handle", new[] { key.RequestType, typeof(CancellationToken) });
            if (method is null)
            {
                throw new MediatorConfigurationException("Handle method not found for " + key.RequestType + ".");
            }

            // H-8 fix: делегат (request, ct) => Task<TResponse> — bound to handler instance
            var delegateType = typeof(Func<,,>).MakeGenericType(
                key.RequestType,
                typeof(CancellationToken),
                typeof(Task<>).MakeGenericType(key.ResponseType));
            return (h, r, ct) =>
            {
                var d = method.CreateDelegate(delegateType, h);
                return (Task)d.DynamicInvoke(r, ct)!;
            };
        });

        var result = invoke(handler, request, cancellationToken);
        return await (Task<TResponse>)result;
    }

    /// <summary>Опубликовать MediatR-уведомление всем хендлерам; M-12 fix — агрегация ошибок.</summary>
    public async ValueTask Publish<TNotification>(TNotification notification, CancellationToken cancellationToken = default)
        where TNotification : global::MediatR.INotification
    {
        Guard(notification);
        var handlers = _serviceProvider.GetServices<global::MediatR.INotificationHandler<TNotification>>();
        List<Exception>? errors = null;
        foreach (var handler in handlers)
        {
            try
            {
                await handler.Handle(notification, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }

    private static void Guard(object? item)
    {
        if (item is null)
        {
            throw new ArgumentNullException(nameof(item));
        }
    }
}

/// <summary>DI-регистрация моста.</summary>
public static class MediatRBridgeRegistration
{
    /// <summary>Зарегистрировать MediatRBridge (хендлеры MediatR должны быть в DI).</summary>
    [RequiresUnreferencedCode("Сканирует сборки: для AOT передайте сборки явно и регистрируйте хендлеры вручную.")]
    public static IServiceCollection AddMedianaMediatRBridge(
        this IServiceCollection services,
        params Assembly[] assemblies)
    {
        services.AddSingleton(sp => new MediatRBridge(sp, assemblies));
        return services;
    }
}
