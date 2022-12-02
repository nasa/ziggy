package gov.nasa.ziggy.ui.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * Invoke methods in the database access thread.
 *
 * @author Sean McCauliff
 */
public class ConsoleProxyInvocationHandler implements InvocationHandler {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ConsoleProxyInvocationHandler.class);

    /**
     * @param o The object that will be wrapped.
     * @return An array of all interfaces that this object implements, including super classes. If
     * there are none than this returns an array of length 0.
     */
    private static Class<?>[] allInterfaces(Object o) {
        List<Class<?>> rv = new ArrayList<>();
        for (Class<?> i = o.getClass(); i != null; i = i.getSuperclass()) {
            Class<?>[] interfaces = i.getInterfaces();
            for (Class<?> interf : interfaces) {
                rv.add(interf);
            }
        }

        Class<?>[] array = new Class[rv.size()];
        return rv.toArray(array);
    }

    public static Object createProxiedObject(Object targetObject) {
        Class<?>[] interfaces = allInterfaces(targetObject);
        ConsoleProxyInvocationHandler handler = new ConsoleProxyInvocationHandler(targetObject);
        return Proxy.newProxyInstance(ConsoleProxyInvocationHandler.class.getClassLoader(),
            interfaces, handler);
    }

    private final Object targetObject;
    private CrudProxyExecutor crudProxyExecutor;

    private ConsoleProxyInvocationHandler(Object targetObject) {
        this.targetObject = targetObject;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {
        return getCrudProxyExecutor().executeSynchronousDatabaseTransaction(() -> {
            return method.invoke(targetObject, args);
        });
    }

    private CrudProxyExecutor getCrudProxyExecutor() {
        if (crudProxyExecutor == null) {
            crudProxyExecutor = ZiggyGuiConsole.crudProxyExecutor;
        }
        return crudProxyExecutor;
    }

    void setCrudProxyExecutor(CrudProxyExecutor crudProxyExecutor) {
        this.crudProxyExecutor = crudProxyExecutor;
    }
}
