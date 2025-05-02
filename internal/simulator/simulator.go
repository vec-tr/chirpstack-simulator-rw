package simulator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-simulator/internal/as"
	"github.com/brocaar/chirpstack-simulator/internal/config"
	"github.com/brocaar/chirpstack-simulator/internal/ns"
	"github.com/brocaar/chirpstack-simulator/simulator"
	"github.com/brocaar/lorawan"
	"github.com/chirpstack/chirpstack/api/go/v4/api"
	"github.com/chirpstack/chirpstack/api/go/v4/common"
	"github.com/chirpstack/chirpstack/api/go/v4/gw"
)

// Start starts the simulator.
func Start(ctx context.Context, wg *sync.WaitGroup, c config.Config) error {
	for i, simConfig := range c.Simulator {
		log.WithFields(log.Fields{
			"i": i,
		}).Info("simulator: starting simulation")

		wg.Add(1)

		pl, err := hex.DecodeString(simConfig.Device.Payload)
		if err != nil {
			return errors.Wrap(err, "decode payload error")
		}

		sim := simulation{
			ctx:                  ctx,
			wg:                   wg,
			tenantID:             simConfig.TenantID,
			deviceCount:          simConfig.Device.Count,
			activationTime:       simConfig.ActivationTime,
			uplinkInterval:       simConfig.Device.UplinkInterval,
			fPort:                simConfig.Device.FPort,
			payload:              pl,
			frequency:            simConfig.Device.Frequency,
			bandwidth:            simConfig.Device.Bandwidth,
			spreadingFactor:      simConfig.Device.SpreadingFactor,
			duration:             simConfig.Duration,
			gatewayMinCount:      simConfig.Gateway.MinCount,
			gatewayMaxCount:      simConfig.Gateway.MaxCount,
			deviceAppKeys:        make(map[lorawan.EUI64]lorawan.AES128Key),
			eventTopicTemplate:   simConfig.Gateway.EventTopicTemplate,
			commandTopicTemplate: simConfig.Gateway.CommandTopicTemplate,
		}

		go sim.start(c)
	}

	return nil
}

type simulation struct {
	ctx             context.Context
	wg              *sync.WaitGroup
	tenantID        string
	deviceCount     int
	gatewayMinCount int
	gatewayMaxCount int
	duration        time.Duration

	fPort           uint8
	payload         []byte
	activationTime  time.Duration
	uplinkInterval  time.Duration
	frequency       int
	bandwidth       int
	spreadingFactor int

	tenant               *api.Tenant
	deviceProfileID      uuid.UUID
	applicationID        string
	gatewayIDs           []lorawan.EUI64
	deviceAppKeysMutex   sync.Mutex
	deviceAppKeys        map[lorawan.EUI64]lorawan.AES128Key
	eventTopicTemplate   string
	commandTopicTemplate string
}

func (s *simulation) start(config config.Config) {
	if err := s.init(config); err != nil {
		log.WithError(err).Error("simulator: init simulation error")
	}

	if err := s.runSimulation(); err != nil {
		log.WithError(err).Error("simulator: simulation error")
	}

	log.Info("simulator: simulation completed")

	if err := s.tearDown(); err != nil {
		log.WithError(err).Error("simulator: tear-down simulation error")
	}

	s.wg.Done()

	log.Info("simulation: tear-down completed")
}

func (s *simulation) init(config config.Config) error {
	log.Info("simulation: setting up")

	if err := s.setupTenant(); err != nil {
		return err
	}

	if err := s.setupGateways(); err != nil {
		return err
	}

	if err := s.setupDeviceProfile(); err != nil {
		return err
	}

	// Pull applications from the configuration and set them up
    for _, app := range config.Simulator[0].Applications { // Assuming the first simulator config
        if err := s.setupApplication(app.Name, "rw-01"); err != nil {
            return err
        }
    }

	if err := s.setupDevices(config.Simulator[0]); err != nil {
		return err
	}

	if err := s.setupApplicationIntegration(); err != nil {
		return err
	}

	return nil
}

func (s *simulation) tearDown() error {
	log.Info("simulation: cleaning up")

	if err := s.tearDownApplicationIntegration(); err != nil {
		return err
	}

	if err := s.tearDownDevices(); err != nil {
		return err
	}

	if err := s.tearDownApplication(); err != nil {
		return err
	}

	if err := s.tearDownDeviceProfile(); err != nil {
		return err
	}

	if err := s.tearDownGateways(); err != nil {
		return err
	}

	return nil
}

func (s *simulation) runSimulation() error {
	var gateways []*simulator.Gateway
	var devices []*simulator.Device

	for _, gatewayID := range s.gatewayIDs {
		gw, err := simulator.NewGateway(
			simulator.WithGatewayID(gatewayID),
			simulator.WithMQTTClient(ns.Client()),
			simulator.WithEventTopicTemplate(s.eventTopicTemplate),
			simulator.WithCommandTopicTemplate(s.commandTopicTemplate),
		)
		if err != nil {
			return errors.Wrap(err, "new gateway error")
		}
		gateways = append(gateways, gw)
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(s.ctx)
	if s.duration != 0 {
		ctx, cancel = context.WithTimeout(ctx, s.duration)
	}
	defer cancel()

	for devEUI, appKey := range s.deviceAppKeys {
		devGateways := make(map[int]*simulator.Gateway)
		devNumGateways := s.gatewayMinCount + mrand.Intn(s.gatewayMaxCount-s.gatewayMinCount+1)

		for len(devGateways) < devNumGateways {
			// pick random gateway index
			n := mrand.Intn(len(gateways))
			devGateways[n] = gateways[n]
		}

		var gws []*simulator.Gateway
		for k := range devGateways {
			gws = append(gws, devGateways[k])
		}

		d, err := simulator.NewDevice(ctx, &wg,
			simulator.WithDevEUI(devEUI),
			simulator.WithAppKey(appKey),
			simulator.WithUplinkInterval(s.uplinkInterval),
			simulator.WithOTAADelay(time.Duration(mrand.Int63n(int64(s.activationTime)))),
			simulator.WithUplinkPayload(false, s.fPort, s.payload),
			simulator.WithGateways(gws),
			simulator.WithUplinkTXInfo(gw.UplinkTxInfo{
				Frequency: uint32(s.frequency),
				Modulation: &gw.Modulation{
					Parameters: &gw.Modulation_Lora{
						Lora: &gw.LoraModulationInfo{
							Bandwidth:       uint32(s.bandwidth),
							SpreadingFactor: uint32(s.spreadingFactor),
							CodeRate:        gw.CodeRate_CR_4_5,
						},
					},
				},
			}),
		)
		if err != nil {
			return errors.Wrap(err, "new device error")
		}

		devices = append(devices, d)
	}

	go func() {
		sigChan := make(chan os.Signal)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		select {
		case sig := <-sigChan:
			log.WithField("signal", sig).Info("signal received, stopping simulators")
			cancel()
		case <-ctx.Done():
		}
	}()

	wg.Wait()

	return nil
}

func (s *simulation) setupTenant() error {
	log.WithFields(log.Fields{
		"tenant_id": s.tenantID,
	}).Info("simulator: retrieving tenant")
	t, err := as.Tenant().Get(context.Background(), &api.GetTenantRequest{
		Id: s.tenantID,
	})
	if err != nil {
		return errors.Wrap(err, "get tenant error")
	}
	s.tenant = t.GetTenant()

	return nil
}

func (s *simulation) setupGateways() error {
	log.Info("simulator: creating gateways")

	for i := 0; i < s.gatewayMaxCount; i++ {
		var gatewayID lorawan.EUI64
		if _, err := rand.Read(gatewayID[:]); err != nil {
			return errors.Wrap(err, "read random bytes error")
		}

		// Assign a meaningful name to the device
		gatewayName := "rw-gw-01"

		_, err := as.Gateway().Create(context.Background(), &api.CreateGatewayRequest{
			Gateway: &api.Gateway{
				GatewayId:   gatewayID.String(),
				Name:        gatewayName,
				Description: gatewayID.String(),
				TenantId:    s.tenant.GetId(),
				Location:    &common.Location{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "create gateway error")
		}

		s.gatewayIDs = append(s.gatewayIDs, gatewayID)
	}

	return nil
}

func (s *simulation) tearDownGateways() error {
	log.Info("simulator: tear-down gateways")

	for _, gatewayID := range s.gatewayIDs {
		_, err := as.Gateway().Delete(context.Background(), &api.DeleteGatewayRequest{
			GatewayId: gatewayID.String(),
		})
		if err != nil {
			return errors.Wrap(err, "delete gateway error")
		}
	}

	return nil
}

func (s *simulation) setupDeviceProfile() error {
	log.Info("simulator: creating device-profile")

	dpName, _ := uuid.NewV4()

	resp, err := as.DeviceProfile().Create(context.Background(), &api.CreateDeviceProfileRequest{
		DeviceProfile: &api.DeviceProfile{
			Name:              dpName.String(),
			TenantId:          s.tenant.GetId(),
			MacVersion:        common.MacVersion_LORAWAN_1_0_3,
			RegParamsRevision: common.RegParamsRevision_B,
			SupportsOtaa:      true,
			Region:            common.Region_EU868,
			AdrAlgorithmId:    "default",
		},
	})
	if err != nil {
		return errors.Wrap(err, "create device-profile error")
	}

	dpID, err := uuid.FromString(resp.Id)
	if err != nil {
		return err
	}
	s.deviceProfileID = dpID

	return nil
}

func (s *simulation) tearDownDeviceProfile() error {
	log.Info("simulator: tear-down device-profile")

	_, err := as.DeviceProfile().Delete(context.Background(), &api.DeleteDeviceProfileRequest{
		Id: s.deviceProfileID.String(),
	})
	if err != nil {
		return errors.Wrap(err, "delete device-profile error")
	}

	return nil
}

func (s *simulation) setupApplication(appName string, applicationID string) error {
    log.WithField("app_name", appName).Info("simulator: creating application")


	createAppResp, err := as.Application().Create(context.Background(), &api.CreateApplicationRequest{
		Application: &api.Application{
			Name:        appName,
			Description: appName,
			TenantId:    s.tenant.GetId(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "create applicaiton error")
	}

	s.applicationID = createAppResp.Id
	fmt.Printf("Created application with ID: %s\n", s.applicationID)
	return nil
}

func (s *simulation) tearDownApplication() error {
	log.Info("simulator: tear-down application")

	_, err := as.Application().Delete(context.Background(), &api.DeleteApplicationRequest{
		Id: s.applicationID,
	})
	if err != nil {
		return errors.Wrap(err, "delete application error")
	}
	return nil
}

func (s *simulation) setupDevices(config config.SimulatorConfig) error {
    log.Info("simulator: init devices")

	if len(config.Device.DevEUIs) < config.Device.Count {
		return errors.New("not enough DevEUIs provided")
	}
    var wg sync.WaitGroup

    for i := 0; i < s.deviceCount; i++ {
        wg.Add(1)

        go func(deviceIndex int) {
            defer wg.Done()

            // Generate a random DevEUI
            // var devEUI lorawan.EUI64
            // if _, err := rand.Read(devEUI[:]); err != nil {
            //     log.Fatal(err)
            // }

			// Parse the DevEUI from the configuration
            var devEUI lorawan.EUI64
            if err := devEUI.UnmarshalText([]byte(config.Device.DevEUIs[deviceIndex])); err != nil {
                log.Fatalf("invalid DevEUI: %s", err)
            }

            // Generate a random AppKey
            var appKey lorawan.AES128Key
            if _, err := rand.Read(appKey[:]); err != nil {
                log.Fatal(err)
            }

            // Assign a meaningful name to the device
            deviceName := fmt.Sprintf("rw-dev-%d", deviceIndex+1)

            // Create the device
            _, err := as.Device().Create(context.Background(), &api.CreateDeviceRequest{
                Device: &api.Device{
                    DevEui:          devEUI.String(),
                    Name:            deviceName,
                    Description:     fmt.Sprintf("Device %d for simulation", deviceIndex+1),
                    ApplicationId:   s.applicationID,
                    DeviceProfileId: s.deviceProfileID.String(),
                },
            })
            if err != nil {
                log.Fatalf("create device error: %s", err)
            }

            // Create the device keys
            _, err = as.Device().CreateKeys(context.Background(), &api.CreateDeviceKeysRequest{
                DeviceKeys: &api.DeviceKeys{
                    DevEui: devEUI.String(),
                    NwkKey: appKey.String(),
                },
            })
            if err != nil {
                log.Fatalf("create device keys error: %s", err)
            }

            // Store the AppKey for the device
            s.deviceAppKeysMutex.Lock()
            s.deviceAppKeys[devEUI] = appKey
            s.deviceAppKeysMutex.Unlock()
        }(i)
    }

    wg.Wait()

    return nil
}

func (s *simulation) tearDownDevices() error {
	log.Info("simulator: tear-down devices")

	for k := range s.deviceAppKeys {
		_, err := as.Device().Delete(context.Background(), &api.DeleteDeviceRequest{
			DevEui: k.String(),
		})
		if err != nil {
			return errors.Wrap(err, "delete device error")
		}
	}

	return nil
}

func (s *simulation) setupApplicationIntegration() error {
	log.Info("simulator: setting up application integration")

	token := as.MQTTClient().Subscribe(fmt.Sprintf("application/%s/device/+/event/up", s.applicationID), 0, func(client mqtt.Client, msg mqtt.Message) {
		applicationUplinkCounter().Inc()
	})
	token.Wait()
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "subscribe application integration error")
	}

	return nil
}

func (s *simulation) tearDownApplicationIntegration() error {
	log.Info("simulator: tear-down application integration")

	token := as.MQTTClient().Unsubscribe(fmt.Sprintf("application/%s/device/+/event/up", s.applicationID))
	token.Wait()
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "unsubscribe application integration error")
	}

	return nil
}
