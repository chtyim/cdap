/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.artifact.ArtifactDescriptor;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipException;
import javax.annotation.Nullable;

/**
 * Inspects a jar file to determine metadata about the artifact.
 */
public class ArtifactInspector {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactInspector.class);
  private final CConfiguration cConf;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ReflectionSchemaGenerator schemaGenerator;

  // TODO: reduce visibility once PluginRepository is replaced by ArtifactRepository
  public ArtifactInspector(CConfiguration cConf) {
    this(cConf, new ArtifactClassLoaderFactory(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile()));
  }

  ArtifactInspector(CConfiguration cConf, ArtifactClassLoaderFactory artifactClassLoaderFactory) {
    this.cConf = cConf;
    this.artifactClassLoaderFactory = artifactClassLoaderFactory;
    this.schemaGenerator = new ReflectionSchemaGenerator();
  }

  /**
   * Inspect the given artifact to determine the classes contained in the artifact.
   * TODO: remove once PluginRepository is gone
   *
   * @param artifactId the id of the artifact to inspect
   * @param artifactFile the artifact file
   * @param template the template this plugin artifact extends
   * @param parentClassLoader the parent classloader to use when inspecting plugins contained in the artifact.
   *                          For example, a ProgramClassLoader created from the artifact the input artifact extends
   * @return metadata about the classes contained in the artifact
   * @throws IOException if there was an exception opening the jar file
   * @throws InvalidArtifactException if inspection failed due to a problem with the artifact, such as a class not
   *                                  found error, or if the application main class isn't really an application
   */
  public ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile, String template,
                                         ClassLoader parentClassLoader) throws IOException, InvalidArtifactException {

    try (PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf, template, parentClassLoader)) {
      return inspectPlugins(ArtifactClasses.builder(), artifactId, artifactFile, pluginInstantiator).build();
    }
  }

  /**
   * Inspect the given artifact to determine the classes contained in the artifact.
   *
   * @param artifactId the id of the artifact to inspect
   * @param artifactFile the artifact file
   * @param parentClassLoader the parent classloader to use when inspecting plugins contained in the artifact.
   *                          For example, a ProgramClassLoader created from the artifact the input artifact extends
   * @return metadata about the classes contained in the artifact
   * @throws IOException if there was an exception opening the jar file
   * @throws InvalidArtifactException if the artifact is invalid. For example, if the application main class is not
   *                                  actually an Application.
   */
  public ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                         ClassLoader parentClassLoader) throws IOException, InvalidArtifactException {

    ArtifactClasses.Builder builder = inspectApplications(artifactId, ArtifactClasses.builder(), artifactFile);

    try (PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf, parentClassLoader)) {
      inspectPlugins(builder, artifactId, artifactFile, pluginInstantiator);
    }

    return builder.build();
  }

  private ArtifactClasses.Builder inspectApplications(Id.Artifact artifactId,
                                                      ArtifactClasses.Builder builder,
                                                      File artifactFile) throws IOException, InvalidArtifactException {

    Location artifactLocation = Locations.toLocation(artifactFile);

    // right now we force users to include the application main class as an attribute in their manifest,
    // which forces them to have a single application class.
    // in the future, we may want to let users do this or maybe specify a list of classes or
    // a package that will be searched for applications, to allow multiple applications in a single artifact.
    String mainClassName;
    try {
      Manifest manifest = BundleJarUtil.getManifest(artifactLocation);
      if (manifest == null) {
        return builder;
      }
      Attributes manifestAttributes = manifest.getMainAttributes();
      if (manifestAttributes == null) {
        return builder;
      }
      mainClassName = manifestAttributes.getValue(ManifestFields.MAIN_CLASS);
    } catch (ZipException e) {
      throw new InvalidArtifactException(String.format(
        "Couldn't unzip artifact %s, please check it is a valid jar file.", artifactId), e);
    }

    if (mainClassName != null) {
      try (CloseableClassLoader artifactClassLoader =
             artifactClassLoaderFactory.createClassLoader(Locations.toLocation(artifactFile))) {

        Object appMain = artifactClassLoader.loadClass(mainClassName).newInstance();
        if (!(appMain instanceof Application)) {
          throw new InvalidArtifactException(String.format("Application main class %s does not implement Application",
            appMain.getClass().getName()));
        }

        Application app = (Application) appMain;

        TypeToken typeToken = TypeToken.of(app.getClass());
        TypeToken<?> resultToken = typeToken.resolveType(Application.class.getTypeParameters()[0]);
        Schema configSchema = null;
        // if the user parameterized their template, like 'xyz extends ApplicationTemplate<T>',
        // we can deserialize the config into that object. Otherwise it'll just be a Config
        if (resultToken.getType() instanceof Class) {
          configSchema = schemaGenerator.generate(resultToken.getType());
        }
        builder.addApp(new ApplicationClass(mainClassName, "", configSchema));
      } catch (ClassNotFoundException e) {
        throw new InvalidArtifactException(String.format(
          "Could not find Application main class %s in artifact %s.", mainClassName, artifactId));
      } catch (UnsupportedTypeException e) {
        throw new InvalidArtifactException(String.format(
          "Config for Application %s in artifact %s has an unsupported schema.", mainClassName, artifactId));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new InvalidArtifactException(String.format(
          "Could not instantiate Application class %s in artifact %s.", mainClassName, artifactId), e);
      }
    }

    return builder;
  }

  /**
   * Inspects the plugin file and extracts plugin classes information.
   */
  private ArtifactClasses.Builder inspectPlugins(ArtifactClasses.Builder builder, Id.Artifact artifactId,
                                                 File artifactFile, PluginInstantiator pluginInstantiator)
    throws IOException, InvalidArtifactException {

    // See if there are export packages. Plugins should be in those packages
    Set<String> exportPackages = getExportPackages(artifactFile);
    if (exportPackages.isEmpty()) {
      return builder;
    }

    // Load the plugin class and inspect the config field.
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(
      artifactId.getName(), artifactId.getVersion(),
      Id.Namespace.SYSTEM.equals(artifactId.getNamespace()),
      Locations.toLocation(artifactFile));

    try {
      ClassLoader pluginClassLoader = pluginInstantiator.getArtifactClassLoader(artifactDescriptor);
      for (Class<?> cls : getPluginClasses(exportPackages, pluginClassLoader)) {
        Plugin pluginAnnotation = cls.getAnnotation(Plugin.class);
        if (pluginAnnotation == null) {
          continue;
        }
        Map<String, PluginPropertyField> pluginProperties = Maps.newHashMap();
        try {
          String configField = getProperties(TypeToken.of(cls), pluginProperties);
          PluginClass pluginClass = new PluginClass(pluginAnnotation.type(), getPluginName(cls),
            getPluginDescription(cls), cls.getName(),
            configField, pluginProperties);
          builder.addPlugin(pluginClass);
        } catch (UnsupportedTypeException e) {
          LOG.warn("Plugin configuration type not supported. Plugin ignored. {}", cls, e);
        }
      }
    } catch (Throwable t) {
      throw new InvalidArtifactException(String.format(
        "Class could not be found while inspecting artifact for plugins. " +
        "Please check dependencies are available, and that the correct parent artifact was specified. " +
        "Error class: %s, message: %s.", t.getClass(), t.getMessage()), t);
    }

    return builder;
  }

  /**
   * Returns the set of package names that are declared in "Export-Package" in the jar file Manifest.
   */
  private Set<String> getExportPackages(File file) throws IOException {
    try (JarFile jarFile = new JarFile(file)) {
      return ManifestFields.getExportPackages(jarFile.getManifest());
    }
  }

  /**
   * Returns an {@link Iterable} of class name that are under the given list of package names that are loadable
   * through the plugin ClassLoader.
   */
  private Iterable<Class<?>> getPluginClasses(final Iterable<String> packages, final ClassLoader pluginClassLoader) {
    return new Iterable<Class<?>>() {
      @Override
      public Iterator<Class<?>> iterator() {
        final Iterator<String> packageIterator = packages.iterator();

        return new AbstractIterator<Class<?>>() {
          Iterator<String> classIterator = ImmutableList.<String>of().iterator();
          String currentPackage;

          @Override
          protected Class<?> computeNext() {
            while (!classIterator.hasNext()) {
              if (!packageIterator.hasNext()) {
                return endOfData();
              }
              currentPackage = packageIterator.next();

              try {
                // Gets all package resource URL for the given package
                String resourceName = currentPackage.replace('.', File.separatorChar);
                Enumeration<URL> resources = pluginClassLoader.getResources(resourceName);
                while (resources.hasMoreElements()) {
                  URL packageResource = resources.nextElement();

                  // Only inspect classes in the top level jar file for Plugins.
                  // The jar manifest may have packages in Export-Package that are loadable from the bundled jar files,
                  // which is for classloading purpose. Those classes won't be inspected for plugin classes.
                  // There should be exactly one of resource that match, because it maps to a directory on the FS.
                  if (packageResource.getProtocol().equals("file")) {
                    classIterator = DirUtils.list(new File(packageResource.toURI()), "class").iterator();
                    break;
                  }
                }
              } catch (Exception e) {
                // Cannot happen
                throw Throwables.propagate(e);
              }
            }

            try {
              return pluginClassLoader.loadClass(getClassName(currentPackage, classIterator.next()));
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
              // Cannot happen, since the class name is from the list of the class files under the classloader.
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }

  /**
   * Extracts and returns name of the plugin.
   */
  private String getPluginName(Class<?> cls) {
    Name annotation = cls.getAnnotation(Name.class);
    return annotation == null || annotation.value().isEmpty() ? cls.getName() : annotation.value();
  }

  /**
   * Returns description for the plugin.
   */
  private String getPluginDescription(Class<?> cls) {
    Description annotation = cls.getAnnotation(Description.class);
    return annotation == null ? "" : annotation.value();
  }

  /**
   * Constructs the fully qualified class name based on the package name and the class file name.
   */
  private String getClassName(String packageName, String classFileName) {
    return packageName + "." + classFileName.substring(0, classFileName.length() - ".class".length());
  }

  /**
   * Gets all config properties for the given plugin.
   *
   * @return the name of the config field in the plugin class or {@code null} if the plugin doesn't have a config field
   */
  @Nullable
  private String getProperties(TypeToken<?> pluginType,
                               Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    // Get the config field
    for (TypeToken<?> type : pluginType.getTypes().classes()) {
      for (Field field : type.getRawType().getDeclaredFields()) {
        TypeToken<?> fieldType = TypeToken.of(field.getGenericType());
        if (PluginConfig.class.isAssignableFrom(fieldType.getRawType())) {
          // Pick up all config properties
          inspectConfigField(fieldType, result);
          return field.getName();
        }
      }
    }
    return null;
  }

  /**
   * Inspects the plugin config class and build up a map for {@link PluginPropertyField}.
   *
   * @param configType type of the config class
   * @param result map for storing the result
   * @throws UnsupportedTypeException if a field type in the config class is not supported
   */
  private void inspectConfigField(TypeToken<?> configType,
                                  Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    for (TypeToken<?> type : configType.getTypes().classes()) {
      if (PluginConfig.class.equals(type.getRawType())) {
        break;
      }

      for (Field field : type.getRawType().getDeclaredFields()) {
        PluginPropertyField property = createPluginProperty(field, type);
        if (result.containsKey(property.getName())) {
          throw new IllegalArgumentException("Plugin config with name " + property.getName()
            + " already defined in " + configType.getRawType());
        }
        result.put(property.getName(), property);
      }
    }
  }

  /**
   * Creates a {@link PluginPropertyField} based on the given field.
   */
  private PluginPropertyField createPluginProperty(Field field,
                                                   TypeToken<?> resolvingType) throws UnsupportedTypeException {
    TypeToken<?> fieldType = resolvingType.resolveType(field.getGenericType());
    Class<?> rawType = fieldType.getRawType();

    Name nameAnnotation = field.getAnnotation(Name.class);
    Description descAnnotation = field.getAnnotation(Description.class);
    String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
    String description = descAnnotation == null ? "" : descAnnotation.value();

    if (rawType.isPrimitive()) {
      return new PluginPropertyField(name, description, rawType.getName(), true);
    }

    rawType = Primitives.unwrap(rawType);
    if (!rawType.isPrimitive() && !String.class.equals(rawType)) {
      throw new UnsupportedTypeException("Only primitive and String types are supported");
    }

    boolean required = true;
    for (Annotation annotation : field.getAnnotations()) {
      if (annotation.annotationType().getName().endsWith(".Nullable")) {
        required = false;
        break;
      }
    }

    return new PluginPropertyField(name, description, rawType.getSimpleName().toLowerCase(), required);
  }
}
