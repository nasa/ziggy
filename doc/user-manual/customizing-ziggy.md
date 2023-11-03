<!-- -*-visual-line-*- -->

[[Previous]](nicknames.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)

## Customizing Ziggy

The [Remote Parameters](remote-parameters.md) article discussed a Java class in Ziggy called `gov.nasa.ziggy.module.remote.RemoteParameters`. You can define your own parameter sets if the `DefaultParameters` is insufficient for your needs. This article discusses how you can do this and more.

### Adding the Ziggy Dependency to Your Build

First, you have to add dependencies to the Ziggy libraries to your build. To do this, run `./gradlew publish` in the Ziggy main directory. Then, add the following to your `build.gradle` file. Your build now has access to the [entire Ziggy Java API](link-to-Javadocs.html).

```groovy
repositories {
    mavenCentral()
    maven {
        url System.getenv('ZIGGY_HOME') + "/repository"
        metadataSources {
            gradleMetadata()
        }
    }
}
dependencies {
    implementation 'gov.nasa:ziggy:+'
}
```

### Adding a Custom Parameter Class

### Other Customizations You Can Make

List the other things that TESS does to give the reader some ideas.

### Using Ziggy Tools in Your Build

Ziggy has a handful of tools to help build code. A few of the more popular tools include ZiggyCpp for compiling C++, ZiggyCppMex for MATLAB fans, and `ZiggySchemaExport` for exporting database schemas from Hibernate classes. For more information, please refer to the [Ziggy buildSrc Java API](link-to-Javadocs.html).

If you want to use any of these tools, add the following to your `build.gradle` file. This also requires that you run `./gradlew publish` in the Ziggy main directory.

```groovy
buildscript {
    repositories {
        mavenCentral()
        maven {
            url System.getenv('ZIGGY_HOME') + "/repository"
            metadataSources {
                gradleMetadata()
            }
        }
    }
    dependencies {
        classpath 'gov.nasa:ziggy-buildSrc:+'
    }
}
```

The API contains examples for using these tools.

[[Previous]](nicknames.md)
[[Up]](dusty-corners.md)
[[Next]](contact-us.md)
