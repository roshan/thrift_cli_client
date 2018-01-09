package com.arjie.thrift_cli_client;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


/**
 * A Thrift Client for an arbitrary service.
 *
 * Either works interactively, or reads from a command file
 *
 * Usage: java -cp your.jar:thrift_cli_client.jar com.arjie.thrift_cli_client.ThriftCliClient hostname port client_class method_name [< input_file]
 *
 *  - client_class is the Thrift-generated client class, something like com.arjie.example.ThriftExample$Client
 *
 *
 *  Unfortunately, because of type erasure, quite a few things must be specified.
 *  Still, I use this using the input file option to just query arbitrary services without having to compile a JAR with
 *  the same serializer each time.
 */
public class ThriftCliClient {

  private static final Charset CHARSET = Charset.forName("UTF-8");

  public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, TException, IllegalAccessException, InvocationTargetException, InstantiationException, FileNotFoundException {
    final String hostName = args[0];
    int port = Integer.valueOf(args[1]);
    final String clientClassName = args[2];
    final String methodName = args[3];

    final InputStream in = System.in;

    @SuppressWarnings("unchecked") final Class<?> clientClass = Class.forName(clientClassName);
    TTransport transport = new TFramedTransport(new TSocket(hostName, port, 1000));
    transport.open();

    final Object clientInstance = clientClass.getConstructor(TProtocol.class).newInstance(new TCompactProtocol(transport));
    final Method method = getMatchingMethod(clientClass, methodName);
    List<Object> params = getParams(method, in);
    System.out.println(String.format("Querying %s.%s(%s)", clientClass.getName(), method.getName(), params));
    final Object result = method.invoke(clientInstance, params.toArray());
    System.out.println(result);
  }

  @SuppressWarnings("unchecked")
  private static List<Object> getParams(Method method, InputStream in) throws IllegalAccessException, InstantiationException, TException, ClassNotFoundException {
    final Parameter[] parameters = method.getParameters();
    List<Object> values = new ArrayList<>();
    final Scanner inputScanner = new Scanner(in);
    final TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

    for (Parameter parameter : parameters) {
      final String name = parameter.getName();
      final Class<?> type = parameter.getType();
      Object paramV = getParam(inputScanner, deserializer, name, type);
      System.out.println(String.format("Adding %s to params for %s", paramV, name));
      values.add(paramV);
    }

    return values;
  }

  private static Object getParam(Scanner inputScanner, TDeserializer deserializer, String name, Class<?> type) throws InstantiationException, IllegalAccessException, TException, ClassNotFoundException {
    Object paramV;
    if (TBase.class.isAssignableFrom(type)) {
      paramV = getTbase(inputScanner, deserializer, name, type);
    } else if (List.class.isAssignableFrom(type)) {
      paramV = getCollection(inputScanner, deserializer, name, new ArrayList<>());
    } else if (Set.class.isAssignableFrom(type)) {
      paramV = getCollection(inputScanner, deserializer, name, new HashSet<>());
    } else if (Enum.class.isAssignableFrom(type)) {
      paramV = getEnum(inputScanner, name, type);
    } else {
      throw new IllegalArgumentException("Cannot support non Thrift types") ;
    }
    return paramV;
  }

  @SuppressWarnings("unchecked")
  private static Enum getEnum(Scanner inputScanner, String name, Class<?> type) {
    System.out.printf(String.format("Enter Thrift Enum value for %s(%s), (Enter blank to stop): ", name, type));
    final String enumVal = inputScanner.nextLine();
    if (enumVal.trim().isEmpty()) {
      return null;
    } else {
      return Enum.valueOf((Class<Enum>)type, enumVal);
    }
  }

  @SuppressWarnings("unchecked")
  private static Collection<Object> getCollection(Scanner inputScanner, TDeserializer deserializer, String name, Collection<Object> collToFill) throws ClassNotFoundException, InstantiationException, IllegalAccessException, TException {
    System.out.printf(String.format("Enter true type for %s (Enter blank for empty collection): ", name));
    final String classNameForTrueType = inputScanner.nextLine();
    if (classNameForTrueType.trim().isEmpty()) {
      return null;
    } else {
      final Class<TBase> trueType = (Class<TBase>)Class.forName(classNameForTrueType);
      System.out.println(String.format("Selecting %s for true type for %s", trueType, name));
      System.out.printf(String.format("Begin to enter %s (%s of %s): ", name, collToFill.getClass().getSimpleName(), trueType));
      while (true) {
        final Object paramV = getParam(inputScanner, deserializer, name, trueType);
        if (paramV == null) {
          break;
        } else {
          collToFill.add(paramV);
        }
      }
      System.out.println(String.format("Completed a %s of %s of size %d", collToFill.getClass().getSimpleName(), trueType, collToFill.size()));
      return collToFill;
    }
  }

  private static TBase getTbase(Scanner inputScanner, TDeserializer deserializer, String name, Class<?> type) throws InstantiationException, IllegalAccessException, TException {
    System.out.printf(String.format("Enter Thrift JSON value for %s(%s), (Enter blank to not enter): ", name, type));
    final String jsonEncodedValue = inputScanner.nextLine();
    if (jsonEncodedValue.trim().isEmpty()) {
      return null;
    } else {
      final TBase proto = (TBase)type.newInstance();
      deserializer.deserialize(proto, jsonEncodedValue.getBytes(CHARSET));
      return proto;
    }
  }

  /**
   * Safe to ignore overloaded methods since Thrift doesn't support those
   */
  private static <T> Method getMatchingMethod(Class<T> klass, String methodName) {
    final Method[] methods = klass.getMethods();
    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        return method;
      }
    }

    throw new IllegalArgumentException("Couldn't find method named " + methodName + " in " + klass);
  }
}
