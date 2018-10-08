package core

import (
	"fmt"

	goavro "gopkg.in/linkedin/goavro.v2"
)

func GetCodecFixture() (codec *goavro.Codec) {
	codec, err := goavro.NewCodec(`{
		"type": "record",
		"name": "Location",
		"fields": [
			{ "name": "accuracy", "type": ["null", "double"], "default": null },
			{ "name": "altitude", "type": ["null", "double"], "default": null },
			{ "name": "altitudeAccuracy", "type": ["null", "double"], "default": null },
			{ "name": "course", "type": ["null", "double"], "default": null },
			{
				"name": "features",
				"type": {
					"type": "array",
					"items": { "name": "id", "type": "string" }
				}
			},
			{ "name": "latitude", "type": "double" },
			{ "name": "longitude", "type": "double" },
			{ "name": "speed", "type": ["null", "double"], "default": null },
			{ "name": "source", "type": "string", "default": "device" },
			{ "name": "timestamp", "type": "long" },
			{ "name": "user_id", "type": "string" }
		]
	}`)

	if err != nil {
		fmt.Printf("failed to create codec for test: %s", err)
	}

	return codec
}

func GetFixtureMap() map[string]interface{} {
	return map[string]interface{}{
		"timestamp": int64(100000),
		"user_id":   "userid1",
	}
}

func GetNativeFixture() (native interface{}) {

	fixtureMap := GetFixtureMap()

	textualTemplate := `{
		"user_id":"%s",
		"timestamp":%d,
		"latitude":37.0,
		"longitude":-121.0,
		"source":"device",
		"features":["osm-2332"]
	}`

	textual := fmt.Sprintf(textualTemplate, fixtureMap["user_id"].(string), fixtureMap["timestamp"].(int64))
	textualAsBytes := []byte(textual)

	// Convert textual Avro data (in Avro JSON format) to native Go form
	codec := GetCodecFixture()
	native, _, err := codec.NativeFromTextual(textualAsBytes)
	if err != nil {
		fmt.Printf("creating native from textual failed: %+v\n", err)
	}

	return native
}
