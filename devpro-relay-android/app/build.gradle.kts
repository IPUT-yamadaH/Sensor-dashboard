plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
}


val serverProperties = java.util.Properties().apply {
    val configFile = rootProject.file("server.properties")
    if (configFile.isFile) configFile.inputStream().use { load(it) }
}
val serverHost = serverProperties.getProperty("SENSOR_SERVER_HOST", "10.0.2.2")
val serverPort = serverProperties.getProperty("SENSOR_SERVER_PORT", "8765")


android {
    buildFeatures {
        buildConfig = true
    }

    namespace = "com.example.myapplication81"
    compileSdk = 36

    defaultConfig {
        buildConfigField("String", "SENSOR_SERVER_HOST", "\"${serverHost}\"")
        buildConfigField("int", "SENSOR_SERVER_PORT", serverPort)
        applicationId = "com.example.myapplication81"

        minSdk = 24

        targetSdk = 36
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
}

dependencies {
    // 安定版のcore-ktxを指定
    implementation("androidx.core:core-ktx:1.13.1")

    // その他の標準ライブラリ群
    implementation(libs.androidx.appcompat)
    implementation(libs.material)
    implementation(libs.androidx.activity)
    implementation(libs.androidx.constraintlayout)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
}
