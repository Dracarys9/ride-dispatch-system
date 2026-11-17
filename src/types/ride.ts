export type RideStatus = "REQUESTED" | "ASSIGNED" | "NO_DRIVER_FOUND";

export interface Driver {
  id: string;
  name: string;
  vehicleNumber: string;
  rating: number;
}

export interface DispatchAttempt {
  attemptNumber: number;
  driverId: string;
  driverName: string;
  accepted: boolean;
  timestamp: string;
}

export interface Ride {
  rideId: string;
  riderId: string;
  pickupLocation: string;
  dropoffLocation: string;
  status: RideStatus;
  assignedDriverId: string | null;
  attempts: DispatchAttempt[];
  createdAt: string;
  updatedAt: string;
  forceReject?: boolean;
}

export interface RideEvent {
  eventId: string;
  rideId: string;
  status: RideStatus;
  driverId: string | null;
  timestamp: string;
}

export interface CreateRideInput {
  riderId?: string;
  pickupLocation?: string;
  dropoffLocation?: string;
  forceReject?: boolean;
}