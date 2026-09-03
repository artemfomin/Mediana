using System.Reflection;
using System.Diagnostics.CodeAnalysis;
using MediatR;
using Mediana.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Mediana.MediatR;

/// <summary>
/// MediatR 12+/14+: inand MediatR- inby Mediana without fromnotand
/// See English documentation.
/// See English documentation.
/// bridge MediatR-behaviors andandin in docs/QUESTIONS.md (Q8)
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

    [RequiresUnreferencedCode("Scans MediatR handlers: for AOT register manually.")]
    private void Scan(Assembly assembly)
    {
        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            // H-7 fix: plugin-onandand — andbut already and
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

    // H-8/M-13/M-15 fix: andfromandin thenin inthen reflection on each inin
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(Type RequestType, Type ResponseType), Func<object, object, CancellationToken, Task>> HandleCache = new();

    /// <summary>Execute a MediatR request through the Mediana bridge (delegate cache, exceptions as-is).</summary>
 [RequiresDynamicCode("MakeGenericType: for AOT and and inbut.")]

 [RequiresUnreferencedCode(" andin in handlers: for trimming — inon and and .")]

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
            var closedInterface = typeof(global::MediatR.IRequestHandler<,>).MakeGenericType(key.RequestType, key.ResponseType);
            var method = closedInterface.GetMethod("Handle", new[] { key.RequestType, typeof(CancellationToken) })
                ?? throw new MediatorConfigurationException("Handle method not found for " + key.RequestType + ".");

            // R5/H-8 fix: Expression.Lambda → compiled delegate, without Invoke/DynamicInvoke — andand as-is
            var handlerParam = System.Linq.Expressions.Expression.Parameter(typeof(object), "handler");
            var requestParam = System.Linq.Expressions.Expression.Parameter(typeof(object), "request");
            var ctParam = System.Linq.Expressions.Expression.Parameter(typeof(CancellationToken), "ct");

            var castHandler = System.Linq.Expressions.Expression.Convert(handlerParam, closedInterface);
            var castRequest = System.Linq.Expressions.Expression.Convert(requestParam, key.RequestType);
            var call = System.Linq.Expressions.Expression.Call(castHandler, method, castRequest, ctParam);
            var castResult = System.Linq.Expressions.Expression.Convert(call, typeof(Task));

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<object, object, CancellationToken, Task>>(
                castResult, handlerParam, requestParam, ctParam);
            return lambda.Compile();
        });

        var result = invoke(handler, request, cancellationToken);
        return await (Task<TResponse>)result;
    }

    /// <summary>Publish a MediatR notification to all handlers; M-12 fix — error aggregation.</summary>
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

/// <summary>DI-andand .</summary>
public static class MediatRBridgeRegistration
{
    /// <summary>andandin MediatRBridge ( MediatR to in DI).</summary>
 [RequiresUnreferencedCode(" and and: for AOT and inbut and and and in .")]

    public static IServiceCollection AddMedianaMediatRBridge(
        this IServiceCollection services,
        params Assembly[] assemblies)
    {
        services.AddSingleton(sp => new MediatRBridge(sp, assemblies));
        return services;
    }
}
